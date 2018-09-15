/*

Copyright 2017 The Wallaroo Authors.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 implied. See the License for the specific language governing
 permissions and limitations under the License.

*/

use "buffered"
use "collections"
use "time"
use "files"
use "wallaroo_labs/bytes"
use "wallaroo_labs/time"
use "wallaroo/core/boundary"
use "wallaroo/core/common"
use "wallaroo/ent/barrier"
use "wallaroo/ent/data_receiver"
use "wallaroo/ent/network"
use "wallaroo/ent/recovery"
use "wallaroo/ent/router_registry"
use "wallaroo/ent/checkpoint"
use "wallaroo_labs/mort"
use "wallaroo/core/messages"
use "wallaroo/core/metrics"
use "wallaroo/core/topology"
use "wallaroo/core/initialization"

class DataChannelListenNotifier is DataChannelListenNotify
  let _name: String
  let _auth: AmbientAuth
  let _is_initializer: Bool
  let _recovery_file: FilePath
  var _host: String = ""
  var _service: String = ""
  let _connections: Connections
  let _metrics_reporter: MetricsReporter
  let _layout_initializer: LayoutInitializer tag
  let _data_receivers: DataReceivers
  let _recovery_replayer: RecoveryReconnecter
  let _router_registry: RouterRegistry
  let _joining_existing_cluster: Bool

  new iso create(name: String, auth: AmbientAuth,
    connections: Connections, is_initializer: Bool,
    metrics_reporter: MetricsReporter iso,
    recovery_file: FilePath,
    layout_initializer: LayoutInitializer tag,
    data_receivers: DataReceivers, recovery_replayer: RecoveryReconnecter,
    router_registry: RouterRegistry, joining: Bool = false)
  =>
    _name = name
    _auth = auth
    _is_initializer = is_initializer
    _connections = connections
    _metrics_reporter = consume metrics_reporter
    _recovery_file = recovery_file
    _layout_initializer = layout_initializer
    _data_receivers = data_receivers
    _recovery_replayer = recovery_replayer
    _router_registry = router_registry
    _joining_existing_cluster = joining

  fun ref listening(listen: DataChannelListener ref) =>
    try
      (_host, _service) = listen.local_address().name()?
      if _host == "::1" then _host = "127.0.0.1" end

      if not _is_initializer then
        _connections.register_my_data_addr(_host, _service)
      end
      _router_registry.register_data_channel_listener(listen)

      @printf[I32]((_name + " data channel: listening on " + _host + ":" +
        _service + "\n").cstring())
      if _recovery_file.exists() then
        @printf[I32]("Recovery file exists for data channel\n".cstring())
      end
      if _joining_existing_cluster then
        //TODO: Do we actually need to do this? Isn't this sent as
        // part of joining worker initialized message?
        let message = ChannelMsgEncoder.identify_data_port(_name, _service,
          _auth)?
        _connections.send_control_to_cluster(message)
      else
        if not _recovery_file.exists() then
          let message = ChannelMsgEncoder.identify_data_port(_name, _service,
            _auth)?
          _connections.send_control_to_cluster(message)
        end
      end
      let f = File(_recovery_file)
      f.print(_host)
      f.print(_service)
      f.sync()
      f.dispose()
    else
      @printf[I32]((_name + " data: couldn't get local address\n").cstring())
      listen.close()
    end

  fun ref not_listening(listen: DataChannelListener ref) =>
    (let h, let s) = try
      listen.local_address().name(None, false)?
    else
      listen.requested_address()
    end
    @printf[I32]((_name + " data: unable to listen on (%s:%s)\n").cstring(),
      h.cstring(), s.cstring())
    Fail()

  fun ref connected(
    listen: DataChannelListener ref,
    router_registry: RouterRegistry): DataChannelNotify iso^
  =>
    DataChannelConnectNotifier(_connections, _auth,
    _metrics_reporter.clone(), _layout_initializer, _data_receivers,
    _recovery_replayer, router_registry)

class DataChannelConnectNotifier is DataChannelNotify
  let _connections: Connections
  let _auth: AmbientAuth
  var _header: Bool = true
  let _metrics_reporter: MetricsReporter
  let _layout_initializer: LayoutInitializer tag
  let _data_receivers: DataReceivers
  let _recovery_replayer: RecoveryReconnecter
  let _router_registry: RouterRegistry

  let _queue: Array[Array[U8] val] = _queue.create()

  // Initial state is an empty DataReceiver wrapper that should never
  // be used (we fail if it is).
  var _receiver: _DataReceiverWrapper = _InitDataReceiver

  new iso create(connections: Connections, auth: AmbientAuth,
    metrics_reporter: MetricsReporter iso,
    layout_initializer: LayoutInitializer tag,
    data_receivers: DataReceivers, recovery_replayer: RecoveryReconnecter,
    router_registry: RouterRegistry)
  =>
    _connections = connections
    _auth = auth
    _metrics_reporter = consume metrics_reporter
    _layout_initializer = layout_initializer
    _data_receivers = data_receivers
    _recovery_replayer = recovery_replayer
    _router_registry = router_registry
    _receiver = _WaitingDataReceiver(_auth, this, _data_receivers)

  fun ref queue_msg(msg: Array[U8] val) =>
    _queue.push(msg)

  fun ref identify_data_receiver(dr: DataReceiver, sender_boundary_id: U128,
    highest_seq_id: SeqId, conn: DataChannel ref)
  =>
    """
    Each abstract data channel (a connection from an OutgoingBoundary)
    corresponds to a single DataReceiver. On reconnect, we want a new
    DataChannel for that boundary to use the same DataReceiver. This is
    called once we have found (or initially created) the DataReceiver for
    the DataChannel corresponding to this notify.
    """
    // State change to our real DataReceiver.
    _receiver = _DataReceiver(_auth, _connections, _metrics_reporter.clone(),
      _layout_initializer, _data_receivers, _recovery_replayer,
      _router_registry, this, dr)
    dr.data_connect(sender_boundary_id, highest_seq_id, conn)
    //!@
    if _queue.size() > 0 then @printf[I32]("!@ DataChannel: playing queued msgs:\n".cstring()) end
    for msg in _queue.values() do
      @printf[I32]("!@ -- Playing msg size %s\n".cstring(), msg.size().string().cstring())
      _receiver.decode_and_process(conn, msg)
    end
    _queue.clear()

    conn._unmute(this)

  fun ref received(conn: DataChannel ref, data: Array[U8] iso): Bool
  =>
    if _header then
      ifdef "trace" then
        @printf[I32]("Rcvd msg header on data channel\n".cstring())
      end
      try
        let expect =
          Bytes.to_u32(data(0)?, data(1)?, data(2)?, data(3)?).usize()

        conn.expect(expect)
        _header = false
      end
      true
    else
      ifdef "trace" then
        @printf[I32]("Rcvd msg on data channel\n".cstring())
      end
      _receiver.decode_and_process(conn, consume data)

      conn.expect(4)
      _header = true

      ifdef linux then
        true
      else
        false
      end
    end

  fun ref accepted(conn: DataChannel ref) =>
    @printf[I32]("accepted data channel connection\n".cstring())
    conn.set_nodelay(true)
    conn.expect(4)

  fun ref connected(sock: DataChannel ref) =>
    @printf[I32]("incoming connected on data channel\n".cstring())

  fun ref closed(conn: DataChannel ref) =>
    @printf[I32]("DataChannelConnectNotifier: server closed\n".cstring())
    //TODO: Initiate reconnect to downstream node here. We need to
    //      create a new connection in OutgoingBoundary

trait _DataReceiverWrapper
  fun ref decode_and_process(conn: DataChannel ref, data: Array[U8] val) =>
    Fail()

class _InitDataReceiver is _DataReceiverWrapper

class _WaitingDataReceiver is _DataReceiverWrapper
  let _auth: AmbientAuth
  let _dccn: DataChannelConnectNotifier ref
  let _data_receivers: DataReceivers

  new create(auth: AmbientAuth, dccn: DataChannelConnectNotifier ref,
    data_receivers: DataReceivers)
  =>
    _auth = auth
    _dccn = dccn
    _data_receivers = data_receivers

  fun ref decode_and_process(conn: DataChannel ref, data: Array[U8] val) =>
    match ChannelMsgDecoder(data, _auth)
    | let dc: DataConnectMsg =>
      ifdef "trace" then
        @printf[I32]("Received DataConnectMsg on Data Channel\n".cstring())
      end
      // Before we can begin processing messages on this data channel, we
      // need to determine which DataReceiver we'll be forwarding data
      // messages to.
      conn._mute(_dccn)
      _data_receivers.request_data_receiver(dc.sender_name,
        dc.sender_boundary_id, dc.highest_seq_id, conn)
    else
      _dccn.queue_msg(data)
    end

class _DataReceiver is _DataReceiverWrapper
  let _auth: AmbientAuth
  let _connections: Connections
  let _metrics_reporter: MetricsReporter
  let _layout_initializer: LayoutInitializer tag
  let _data_receivers: DataReceivers
  let _recovery_replayer: RecoveryReconnecter
  let _router_registry: RouterRegistry
  let _dccn: DataChannelConnectNotifier ref
  let _data_receiver: DataReceiver

  new create(auth: AmbientAuth, connections: Connections,
    metrics_reporter: MetricsReporter iso,
    layout_initializer: LayoutInitializer tag,
    data_receivers: DataReceivers, recovery_replayer: RecoveryReconnecter,
    router_registry: RouterRegistry,
    dccn: DataChannelConnectNotifier ref, dr: DataReceiver)
  =>
    _auth = auth
    _connections = connections
    _metrics_reporter = consume metrics_reporter
    _layout_initializer = layout_initializer
    _data_receivers = data_receivers
    _recovery_replayer = recovery_replayer
    _router_registry = router_registry
    _dccn = dccn
    _data_receiver = dr

  fun ref decode_and_process(conn: DataChannel ref, data: Array[U8] val) =>
    // because we received this from another worker
    let ingest_ts = WallClock.nanoseconds()
    let my_latest_ts = Time.nanos()

    match ChannelMsgDecoder(data, _auth)
    | let data_msg: DataMsg =>
      ifdef "trace" then
        @printf[I32]("Received DataMsg on Data Channel\n".cstring())
      end
      _metrics_reporter.step_metric(data_msg.metric_name,
        "Before receive on data channel (network time)", data_msg.metrics_id,
        data_msg.latest_ts, ingest_ts)
      _data_receiver.received(data_msg.delivery_msg,
          data_msg.pipeline_time_spent + (ingest_ts - data_msg.latest_ts),
          data_msg.seq_id, my_latest_ts, data_msg.metrics_id + 1,
          my_latest_ts)
    | let dc: DataConnectMsg =>
      @printf[I32](("Received DataConnectMsg on DataChannel, but we already " +
        "have a DataReceiver for this connection.\n").cstring())
    | let km: KeyMigrationMsg =>
      ifdef "trace" then
        @printf[I32]("Received KeyMigrationMsg on Data Channel\n".cstring())
      end
      _layout_initializer.receive_immigrant_key(km)
    | let m: MigrationBatchCompleteMsg =>
      ifdef "trace" then
        @printf[I32]("Received MigrationBatchCompleteMsg on Data Channel\n".cstring())
      end
      // Go through layout_initializer and router_registry to make sure
      // pending messages on registry are processed first. That's because
      // the current message path for receiving immigrant steps is
      // layout_initializer then router_registry.
      _layout_initializer.ack_migration_batch_complete(m.sender_name)
    | let aw: AckDataReceivedMsg =>
      ifdef "trace" then
        @printf[I32]("Received AckDataReceivedMsg on Data Channel\n"
          .cstring())
      end
      Fail()
    | let r: ReplayMsg =>
      ifdef "trace" then
        @printf[I32]("Received ReplayMsg on Data Channel\n".cstring())
      end
      try
        match r.msg(_auth)?
        | let data_msg: DataMsg =>
          _metrics_reporter.step_metric(data_msg.metric_name,
            "Before replay receive on data channel (network time)",
            data_msg.metrics_id, data_msg.latest_ts, ingest_ts)
          _data_receiver.replay_received(data_msg.delivery_msg,
            data_msg.pipeline_time_spent + (ingest_ts - data_msg.latest_ts),
            data_msg.seq_id, my_latest_ts, data_msg.metrics_id + 1,
            my_latest_ts)
        | let fbm: ForwardBarrierMsg =>
          _data_receiver.forward_barrier(fbm.target_id, fbm.origin_id,
            fbm.token, fbm.seq_id)
        end
      else
        Fail()
      end
    | let c: ReplayCompleteMsg =>
      ifdef "trace" then
        @printf[I32]("Received ReplayCompleteMsg on Data Channel\n"
          .cstring())
      end
      //!@ We should probably be removing this whole message
      // _recovery_replayer.add_boundary_replay_complete(c.sender_name,
      //   c.boundary_id)
    | let m: SpinUpLocalTopologyMsg =>
      @printf[I32]("Received spin up local topology message!\n".cstring())
    | let m: ReportStatusMsg =>
      _data_receiver.report_status(m.code)
    | let m: RegisterProducerMsg =>
      _data_receiver.register_producer(m.source_id, m.target_id)
    | let m: UnregisterProducerMsg =>
      _data_receiver.unregister_producer(m.source_id, m.target_id)
    | let m: ForwardBarrierMsg =>
      _data_receiver.forward_barrier(m.target_id, m.origin_id, m.token, m.seq_id)
    | let m: UnknownChannelMsg =>
      @printf[I32]("Unknown Wallaroo data message type: UnknownChannelMsg.\n"
        .cstring())
    else
      @printf[I32]("Unknown Wallaroo data message type.\n".cstring())
    end
