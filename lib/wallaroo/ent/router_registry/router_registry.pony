/*

Copyright 2017 The Wallaroo Authors.

Licensed as a Wallaroo Enterprise file under the Wallaroo Community
License (the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

     https://github.com/wallaroolabs/wallaroo/blob/master/LICENSE

*/

use "collections"
use "net"
use "time"
use "wallaroo"
use "wallaroo/core/boundary"
use "wallaroo/core/common"
use "wallaroo/core/data_channel"
use "wallaroo/core/initialization"
use "wallaroo/core/invariant"
use "wallaroo/core/messages"
use "wallaroo/core/metrics"
use "wallaroo/core/routing"
use "wallaroo/core/source"
use "wallaroo/core/source/tcp_source"
use "wallaroo/core/topology"
use "wallaroo/ent/data_receiver"
use "wallaroo/ent/network"
use "wallaroo/ent/recovery"
use "wallaroo_labs/collection_helpers"
use "wallaroo_labs/messages"
use "wallaroo_labs/mort"
use "wallaroo_labs/query"


actor RouterRegistry is InFlightAckRequester
  let _id: StepId
  let _auth: AmbientAuth
  let _data_receivers: DataReceivers
  let _worker_name: String
  let _connections: Connections
  let _recovery_file_cleaner: RecoveryFileCleaner
  var _data_router: DataRouter =
    DataRouter(recover Map[U128, Consumer] end)
  var _pre_state_data: (Array[PreStateData] val | None) = None
  let _partition_routers: Map[String, PartitionRouter] =
    _partition_routers.create()
  let _stateless_partition_routers: Map[U128, StatelessPartitionRouter] =
    _stateless_partition_routers.create()
  let _data_receiver_map: Map[String, DataReceiver] =
    _data_receiver_map.create()

  var _local_topology_initializer: (LocalTopologyInitializer | None) = None

  var _omni_router: (OmniRouter | None) = None

  var _application_ready_to_work: Bool = false

  let _in_flight_ack_waiter: InFlightAckWaiter

  ////////////////
  // Subscribers
  // All steps that have a PartitionRouter, registered by partition
  // state name
  let _partition_router_subs: Map[String, SetIs[RouterUpdateable]] =
      _partition_router_subs.create()
  // All steps that have a StatelessPartitionRouter, registered by
  // partition id
  let _stateless_partition_router_subs:
    Map[U128, SetIs[RouterUpdateable]] =
      _stateless_partition_router_subs.create()
  // All steps that have an OmniRouter
  let _omni_router_steps: SetIs[Step] = _omni_router_steps.create()
  //
  ////////////////

  let _sources: Map[StepId, Source] = _sources.create()
  let _source_listeners: SetIs[SourceListener] = _source_listeners.create()
  // Map from Source digestof value to source id
  let _source_ids: Map[U64, StepId] = _source_ids.create()
  let _data_channel_listeners: SetIs[DataChannelListener] =
    _data_channel_listeners.create()
  let _control_channel_listeners: SetIs[TCPListener] =
    _control_channel_listeners.create()
  let _data_channels: SetIs[DataChannel] = _data_channels.create()
  // Boundary builders are used by new TCPSources to create their own
  // individual boundaries to other workers (to allow for increased
  // throughput).
  let _outgoing_boundaries_builders: Map[String, OutgoingBoundaryBuilder] =
    _outgoing_boundaries_builders.create()
  let _outgoing_boundaries: Map[String, OutgoingBoundary] =
    _outgoing_boundaries.create()

  //////
  // Partition Migration
  //////

  var _stop_the_world_in_process: Bool = false

  var _leaving_in_process: Bool = false
  var _joining_worker_count: USize = 0
  let _initialized_joining_workers: _StringSet =
    _initialized_joining_workers.create()
  // Steps migrated out and waiting for acknowledgement
  let _step_waiting_list: SetIs[U128] = _step_waiting_list.create()
  // Workers in running cluster that have been stopped for migration
  let _stopped_worker_waiting_list: _StringSet =
    _stopped_worker_waiting_list.create()
  // Migration targets that have not yet acked migration batch complete
  let _migration_target_ack_list: _StringSet =
    _migration_target_ack_list.create()

  // For keeping track of leaving workers during an autoscale shrink
  let _leaving_workers_waiting_list: _StringSet =
    _leaving_workers_waiting_list.create()
  var _leaving_workers: Array[String] val = recover Array[String] end
  // Used as a proxy for RouterRegistry when muting and unmuting sources
  // and data channel.
  // TODO: Probably change mute()/unmute() interface so we don't need this
  let _dummy_consumer: DummyConsumer = DummyConsumer

  var _stop_the_world_pause: U64

  var _waiting_to_finish_join: Bool = false

  var _event_log: (EventLog | None) = None

  var _initiated_stop_the_world: Bool = false

  new create(auth: AmbientAuth, worker_name: String,
    data_receivers: DataReceivers, c: Connections,
    recovery_file_cleaner: RecoveryFileCleaner, stop_the_world_pause: U64)
  =>
    _auth = auth
    _worker_name = worker_name
    _data_receivers = data_receivers
    _connections = c
    _recovery_file_cleaner = recovery_file_cleaner
    _stop_the_world_pause = stop_the_world_pause
    _connections.register_disposable(this)
    _id = (digestof this).u128()
    _in_flight_ack_waiter = InFlightAckWaiter(_id)
    _data_receivers.set_router_registry(this)

  fun _worker_count(): USize =>
    _outgoing_boundaries.size() + 1

  be dispose() =>
    None

  be application_ready_to_work() =>
    _application_ready_to_work = true

  be set_data_router(dr: DataRouter) =>
    _data_router = dr
    _distribute_data_router()

  be set_pre_state_data(psd: Array[PreStateData] val) =>
    _pre_state_data = psd

  be set_partition_router(state_name: String, pr: PartitionRouter) =>
    _partition_routers(state_name) = pr

  be set_stateless_partition_router(partition_id: U128,
    pr: StatelessPartitionRouter)
  =>
    _stateless_partition_routers(partition_id) = pr

  be set_omni_router(o: OmniRouter) =>
    _set_omni_router(o)

  fun ref _set_omni_router(o: OmniRouter) =>
    var omn = o
    for (w, dr) in _data_receiver_map.pairs() do
      omn = o.add_data_receiver(w, dr)
    end
    for (w, ob) in _outgoing_boundaries.pairs() do
      omn = o.add_boundary(w, ob)
    end
    for (s_id, s) in _sources.pairs() do
      omn = o.add_source(s_id, s)
    end
    _omni_router = omn

  be set_event_log(e: EventLog) =>
    _event_log = e

  be register_local_topology_initializer(lt: LocalTopologyInitializer) =>
    _local_topology_initializer = lt

  // TODO: We need a new approach to registering all disposable actors.
  // This is a stopgap to register boundaries generated by a Source.
  // See issue #1411.
  be register_disposable(d: DisposableActor) =>
    _connections.register_disposable(d)

  be register_source(source: Source, source_id: StepId) =>
    _sources(source_id) = source
    _source_ids(digestof source) = source_id
    if not _stop_the_world_in_process and _application_ready_to_work then
      match source
      | let tcp_source: TCPSource =>
        ifdef debug then
          @printf[I32]("register_source: resuming ?? source\n".cstring())
        end
        tcp_source.unmute_source(_dummy_consumer)
      end
    end
    match _omni_router
    | let omnr: OmniRouter =>
      _omni_router = omnr.add_source(source_id, source)
    else
      Fail()
    end
    _distribute_omni_router()
    _connections.register_disposable(source)
    _connections.notify_cluster_of_new_source(source_id)

  be register_remote_source(sender: String, source_id: StepId) =>
    match _omni_router
    | let omnr: OmniRouter =>
      _omni_router = omnr.add_source(source_id, ProxyAddress(sender,
        source_id))
    else
      Fail()
    end
    _distribute_omni_router()

  be register_source_listener(source_listener: SourceListener) =>
    _source_listeners.set(source_listener)
    _connections.register_disposable(source_listener)

  be register_data_channel_listener(dchl: DataChannelListener) =>
    _data_channel_listeners.set(dchl)
    if _waiting_to_finish_join and
      (_control_channel_listeners.size() != 0)
    then
      _inform_cluster_of_join()
      _waiting_to_finish_join = false
    end

  be register_control_channel_listener(cchl: TCPListener) =>
    _control_channel_listeners.set(cchl)
    if _waiting_to_finish_join and
      (_data_channel_listeners.size() != 0)
    then
      _inform_cluster_of_join()
      _waiting_to_finish_join = false
    end

  be register_data_channel(dc: DataChannel) =>
    // TODO: These need to be unregistered if they close
    _data_channels.set(dc)

  be register_partition_router_subscriber(state_name: String,
    sub: RouterUpdateable)
  =>
    _register_partition_router_subscriber(state_name, sub)

  fun ref _register_partition_router_subscriber(state_name: String,
    sub: RouterUpdateable)
  =>
    try
      if _partition_router_subs.contains(state_name) then
        _partition_router_subs(state_name)?.set(sub)
      else
        _partition_router_subs(state_name) = SetIs[RouterUpdateable]
        _partition_router_subs(state_name)?.set(sub)
      end
    else
      Fail()
    end

  be unregister_partition_router_subscriber(state_name: String,
    sub: RouterUpdateable)
  =>
    Invariant(_partition_router_subs.contains(state_name))
    try
      _partition_router_subs(state_name)?.unset(sub)
    else
      Fail()
    end

  be register_stateless_partition_router_subscriber(partition_id: U128,
    sub: RouterUpdateable)
  =>
    _register_stateless_partition_router_subscriber(partition_id, sub)

  fun ref _register_stateless_partition_router_subscriber(
    partition_id: U128, sub: RouterUpdateable)
  =>
    try
      if _stateless_partition_router_subs.contains(partition_id) then
        _stateless_partition_router_subs(partition_id)?.set(sub)
      else
        _stateless_partition_router_subs(partition_id) =
          SetIs[RouterUpdateable]
        _stateless_partition_router_subs(partition_id)?.set(sub)
      end
    else
      Fail()
    end

  be unregister_stateless_partition_router_subscriber(partition_id: U128,
    sub: RouterUpdateable)
  =>
    Invariant(_stateless_partition_router_subs.contains(partition_id))
    try
      _stateless_partition_router_subs(partition_id)?.unset(sub)
    else
      Fail()
    end

  be register_omni_router_step(s: Step) =>
    _register_omni_router_step(s)

  fun ref _register_omni_router_step(s: Step) =>
    _omni_router_steps.set(s)

  be register_boundaries(bs: Map[String, OutgoingBoundary] val,
    bbs: Map[String, OutgoingBoundaryBuilder] val)
  =>
    // Boundaries
    let new_boundaries = recover trn Map[String, OutgoingBoundary] end
    for (worker, boundary) in bs.pairs() do
      if not _outgoing_boundaries.contains(worker) then
        _outgoing_boundaries(worker) = boundary
        new_boundaries(worker) = boundary
      end
    end
    let new_boundaries_sendable: Map[String, OutgoingBoundary] val =
      consume new_boundaries

    for producers in _partition_router_subs.values() do
      for producer in producers.values() do
        match producer
        | let s: Step =>
          s.add_boundaries(new_boundaries_sendable)
        end
      end
    end
    for producers in _stateless_partition_router_subs.values() do
      for producer in producers.values() do
        match producer
        | let s: Step =>
          s.add_boundaries(new_boundaries_sendable)
        end
      end
    end
    for step in _omni_router_steps.values() do
      step.add_boundaries(new_boundaries_sendable)
    end
    for step in _data_router.routes().values() do
      match step
      | let s: Step => s.add_boundaries(new_boundaries_sendable)
      end
    end

    // Boundary builders
    let new_boundary_builders =
      recover trn Map[String, OutgoingBoundaryBuilder] end
    for (worker, builder) in bbs.pairs() do
      // Boundary builders should always be registered after the canonical
      // boundary for each builder. The canonical is used on all Steps.
      // Sources use the builders to create a new boundary per source
      // connection.
      if not _outgoing_boundaries.contains(worker) then
        Fail()
      end
      if not _outgoing_boundaries_builders.contains(worker) then
        _outgoing_boundaries_builders(worker) = builder
        new_boundary_builders(worker) = builder
      end
    end

    let new_boundary_builders_sendable:
      Map[String, OutgoingBoundaryBuilder] val =
        consume new_boundary_builders

    for source_listener in _source_listeners.values() do
      source_listener.add_boundary_builders(new_boundary_builders_sendable)
    end

    for source in _sources.values() do
      source.add_boundary_builders(new_boundary_builders_sendable)
    end

  be register_data_receiver(worker: String, dr: DataReceiver) =>
    _data_receiver_map(worker) = dr
    match _omni_router
    | let omnr: OmniRouter =>
      _omni_router = omnr.add_data_receiver(worker, dr)
      _distribute_omni_router()
    end

  fun _distribute_data_router() =>
    _data_receivers.update_data_router(_data_router)

  fun _distribute_omni_router() =>
    try
      for step in _omni_router_steps.values() do
        step.update_omni_router(_omni_router as OmniRouter)
      end
      match _local_topology_initializer
      | let lti: LocalTopologyInitializer =>
        lti.update_omni_router(_omni_router as OmniRouter)
      else
        Fail()
      end
    else
      Fail()
    end

  fun ref _distribute_partition_router(partition_router: PartitionRouter) =>
    let state_name = partition_router.state_name()

    try
      if not _partition_router_subs.contains(state_name) then
        _partition_router_subs(state_name) = SetIs[RouterUpdateable]
      end
      for sub in _partition_router_subs(state_name)?.values() do
        sub.update_router(partition_router)
      end
    else
      Fail()
    end

  fun ref _distribute_stateless_partition_router(
    partition_router: StatelessPartitionRouter)
  =>
    let partition_id = partition_router.partition_id()

    try
      if not _stateless_partition_router_subs.contains(partition_id) then
        _stateless_partition_router_subs(partition_id) =
          SetIs[RouterUpdateable]
      end
      for sub in
        _stateless_partition_router_subs(partition_id)?.values()
      do
        sub.update_router(partition_router)
      end
    else
      Fail()
    end
    match _omni_router
    | let omni: OmniRouter =>
      _omni_router = omni.update_stateless_partition_router(partition_id,
        partition_router)
    else
      Fail()
    end
    _distribute_omni_router()

  fun ref _remove_worker(worker: String) =>
    try
      _data_receiver_map.remove(worker)?
    else
      Fail()
    end
    _remove_worker_from_omni_router(worker)
    _distribute_boundary_removal(worker)

  fun ref _remove_worker_from_omni_router(worker: String) =>
    match _omni_router
    | let omnr: OmniRouter =>
      _omni_router = omnr.remove_boundary(worker).remove_data_receiver(worker)
    end

    _distribute_omni_router()

  fun ref _distribute_boundary_removal(worker: String) =>
    for subs in _partition_router_subs.values() do
      for sub in subs.values() do
        match sub
        | let r: BoundaryUpdateable =>
          r.remove_boundary(worker)
        end
      end
    end
    for subs in _stateless_partition_router_subs.values() do
      for sub in subs.values() do
        match sub
        | let r: BoundaryUpdateable =>
          r.remove_boundary(worker)
        end
      end
    end
    for step in _omni_router_steps.values() do
      step.remove_boundary(worker)
    end

    for source in _sources.values() do
      source.remove_boundary(worker)
    end
    for source_listener in _source_listeners.values() do
      source_listener.remove_boundary(worker)
    end

    match _local_topology_initializer
    | let lt: LocalTopologyInitializer =>
      lt.remove_boundary(worker)
    else
      Fail()
    end

  fun _distribute_boundary_builders() =>
    let boundary_builders =
      recover trn Map[String, OutgoingBoundaryBuilder] end
    for (worker, builder) in _outgoing_boundaries_builders.pairs() do
      boundary_builders(worker) = builder
    end

    let boundary_builders_to_send = consume val boundary_builders

    for source_listener in _source_listeners.values() do
      source_listener.update_boundary_builders(boundary_builders_to_send)
    end

  be create_partition_routers_from_blueprints(
    partition_blueprints: Map[String, PartitionRouterBlueprint] val)
  =>
    let obs_trn = recover trn Map[String, OutgoingBoundary] end
    for (w, ob) in _outgoing_boundaries.pairs() do
      obs_trn(w) = ob
    end
    let obs = consume val obs_trn
    for (s, b) in partition_blueprints.pairs() do
      let next_router = b.build_router(_worker_name, obs, _auth)
       _distribute_partition_router(next_router)
      _partition_routers(s) = next_router
    end

  be create_stateless_partition_routers_from_blueprints(
    partition_blueprints: Map[U128, StatelessPartitionRouterBlueprint] val)
  =>
    let obs_trn = recover trn Map[String, OutgoingBoundary] end
    for (w, ob) in _outgoing_boundaries.pairs() do
      obs_trn(w) = ob
    end
    let obs = consume val obs_trn
    for (id, b) in partition_blueprints.pairs() do
      let next_router = b.build_router(_worker_name, obs, _auth)
      _distribute_stateless_partition_router(next_router)
      _stateless_partition_routers(id) = next_router
    end

  be create_omni_router_from_blueprint(
    omni_router_blueprint: OmniRouterBlueprint,
    local_sinks: Map[StepId, Consumer] val,
    lti: LocalTopologyInitializer)
  =>
    let obs_trn = recover trn Map[String, OutgoingBoundary] end
    for (w, ob) in _outgoing_boundaries.pairs() do
      obs_trn(w) = ob
    end
    let obs = consume val obs_trn
    let new_omni_router = omni_router_blueprint.build_router(_worker_name,
      obs, local_sinks)
    _set_omni_router(new_omni_router)
    lti.set_omni_router(new_omni_router)
    lti.initialize_join_initializables()

  be inform_joining_worker(conn: TCPConnection, worker: String,
    worker_count: USize, local_topology: LocalTopology)
  =>
    if _joining_worker_count == 0 then
      _joining_worker_count = worker_count
    end
    if (_joining_worker_count > 0) and (worker_count != _joining_worker_count)
    then
      @printf[I32]("Join error: Joining worker supplied invalid worker count\n"
        .cstring())
      let error_msg = "All joining workers must supply the same worker " +
        "count. Current pending count is " + _joining_worker_count.string() +
        ". You supplied " + worker_count.string() + "."
      try
        let msg = ChannelMsgEncoder.inform_join_error(error_msg, _auth)?
        conn.writev(msg)
      else
        Fail()
      end
    elseif worker_count < 1 then
      @printf[I32](("Join error: Joining worker supplied a worker count " +
        "less than 1\n").cstring())
      let error_msg = "Joining worker must supply a worker count greater " +
        "than 0."
      try
        let msg = ChannelMsgEncoder.inform_join_error(error_msg, _auth)?
        conn.writev(msg)
      else
        Fail()
      end
    else
      let state_blueprints =
        recover trn Map[String, PartitionRouterBlueprint] end
      for (w, r) in _partition_routers.pairs() do
        state_blueprints(w) = r.blueprint()
      end

      let stateless_blueprints =
        recover trn Map[U128, StatelessPartitionRouterBlueprint] end
      for (id, r) in _stateless_partition_routers.pairs() do
        stateless_blueprints(id) = r.blueprint()
      end

      let omni_router_blueprint =
        match _omni_router
        | let omr: OmniRouter =>
          omr.blueprint()
        else
          Fail()
          EmptyOmniRouterBlueprint
        end

      _connections.inform_joining_worker(conn, worker, local_topology,
        consume state_blueprints, consume stateless_blueprints,
        omni_router_blueprint)
    end

  be inform_cluster_of_join() =>
    _inform_cluster_of_join()

  fun ref _inform_cluster_of_join() =>
    if (_data_channel_listeners.size() != 0) and
       (_control_channel_listeners.size() != 0)
    then
      _connections.inform_cluster_of_join()
    else
      _waiting_to_finish_join = true
    end

  be inform_worker_of_boundary_count(target_worker: String) =>
    // There is one boundary per source plus the canonical boundary
    let count = _sources.size() + 1
    _connections.inform_worker_of_boundary_count(target_worker, count)

  be reconnect_source_boundaries(target_worker: String) =>
    for source in _sources.values() do
      source.reconnect_boundary(target_worker)
    end

  be partition_query(conn: TCPConnection) =>
    let msg = ExternalMsgEncoder.partition_query_response(
      _partition_routers, _stateless_partition_routers)
    conn.writev(msg)

  be partition_count_query(conn: TCPConnection) =>
    let msg = ExternalMsgEncoder.partition_count_query_response(
      _partition_routers, _stateless_partition_routers)
    conn.writev(msg)

  be cluster_status_query_not_initialized(conn: TCPConnection) =>
    let msg = ExternalMsgEncoder.cluster_status_query_reponse_not_initialized()
    conn.writev(msg)

  be cluster_status_query(worker_names: Array[String] val,
    conn: TCPConnection)
  =>
    let msg = ExternalMsgEncoder.cluster_status_query_response(
      worker_names.size(), worker_names, _stop_the_world_in_process)
    conn.writev(msg)

  be source_ids_query(conn: TCPConnection) =>
    let ids = recover iso Array[String] end
    for s_id in _source_ids.values() do
      ids.push(s_id.string())
    end
    let msg = ExternalMsgEncoder.source_ids_query_response(
      consume ids)
    conn.writev(msg)

  //////////////
  // LOG ROTATION
  //////////////
  be rotate_log_file() =>
    """
    Called when it's time to rotate the log file for the worker.
    This will mute upstream, ack on all in-flight messages, then initiate a
    log file rotation, followed by snapshotting of all states on the worker
    to the new file, before unmuting upstream and resuming processing.
    """
    _initiated_stop_the_world = true
    _stop_the_world_in_process = true
    _stop_the_world_for_log_rotation()

    _initiate_request_in_flight_acks(LogRotationAction(this))

  be begin_log_rotation() =>
    """
    Start the log rotation and initiate snapshots.
    """
    match _event_log
    | let e: EventLog =>
      e.rotate_file()
    else
      Fail()
    end

  be rotation_complete() =>
    """
    Called when rotation has completed and we should resume processing
    """
    _connections.request_cluster_unmute()
    _unmute_request(_worker_name)

  fun ref _stop_the_world_for_log_rotation() =>
    """
    We currently stop all message processing before perofrming log rotaion.
    """
    @printf[I32]("~~~Stopping message processing for log rotation.~~~\n"
      .cstring())
    _mute_request(_worker_name)
    _connections.stop_the_world()

  //////////////
  // NEW WORKER PARTITION MIGRATION
  //////////////
  be joining_worker_initialized(worker: String) =>
    """
    When a joining worker has initialized its topology, it contacts all
    current workers. This behavior is called when that control
    message is received.
    """
    if _joining_worker_count == 0 then
      ifdef debug then
        @printf[I32](("Joining worker reported as initialized, but we're " +
          "not waiting for it. This should mean that either we weren't the " +
          "worker contacted for the join or we're also joining.\n").cstring())
      end
      return
    end
    _initialized_joining_workers.set(worker)
    if _initialized_joining_workers.size() == _joining_worker_count then
      let nws = recover trn Array[String] end
      for w in _initialized_joining_workers.values() do
        nws.push(w)
      end
      let new_workers = consume val nws
      _connections.notify_joining_workers_of_joining_addresses(new_workers)
      try
        let msg =
          ChannelMsgEncoder.initiate_join_migration(new_workers, _auth)?
        _connections.send_control_to_cluster(msg)
      else
        Fail()
      end
      // TODO: Where should this line go?
      _initiated_stop_the_world = true

      _migrate_onto_new_workers(new_workers)
      _initialized_joining_workers.clear()
      _joining_worker_count = 0
    end

  be remote_migration_request(new_workers: Array[String] val) =>
    """
    Only one worker is contacted by all joining workers to indicate that a
    join is requested. That worker, when it's ready to begin step migration,
    then sends a message to all other current workers, telling them to begin
    migration to the joining workers as well. This behavior is called when
    that message is received.
    """
    if not ArrayHelpers[String].contains[String](new_workers, _worker_name)
    then
      _migrate_onto_new_workers(new_workers)
    end

  fun ref _migrate_onto_new_workers(new_workers: Array[String] val) =>
    """
    Called when a new worker joins the cluster and we are ready to start
    the partition migration process. We first trigger a pause to allow
    in-flight messages to finish processing.
    """
    _stop_the_world_in_process = true
    _stop_the_world_for_grow_migration(new_workers)
    _initiate_request_in_flight_acks(MigrationAction(this, new_workers))

  fun ref _stop_the_world_for_grow_migration(new_workers: Array[String] val) =>
    """
    We currently stop all message processing before migrating partitions and
    updating routers/routes.
    """
    @printf[I32]("~~~Stopping message processing for state migration.~~~\n"
      .cstring())
    for w in new_workers.values() do
      _migration_target_ack_list.set(w)
    end
    _mute_request(_worker_name)
    _connections.stop_the_world(new_workers)

  fun ref _try_resume_the_world() =>
    if _initiated_stop_the_world then
      // Since we don't need all steps to be up to date on the OmniRouter
      // during migrations, we wait on the worker that initiated stop the
      // world to distribute it until here to avoid unnecessary messaging.
      _distribute_omni_router()
      let in_flight_resume_ack_id = _in_flight_ack_waiter
        .initiate_resume_request(ResumeTheWorldAction(this))
      _request_in_flight_resume_acks(in_flight_resume_ack_id,
        _leaving_workers)
      _connections.request_in_flight_resume_acks(in_flight_resume_ack_id,
        _id, _leaving_workers, this)
      // We are done with this round of leaving workers
      _leaving_workers = recover Array[String] end
    end

  fun ref initiate_resume_the_world() =>
    _resume_all_remote()
    _resume_the_world(_worker_name)

  be resume_the_world(initiator: String) =>
    """
    Stop the world is complete and we're ready to resume message processing
    """
    _resume_the_world(initiator)

  fun ref _resume_the_world(initiator: String) =>
    _initiated_stop_the_world = false
    _stop_the_world_in_process = false
    _resume_all_local()
    @printf[I32]("~~~Resuming message processing.~~~\n".cstring())

  be begin_migration(target_workers: Array[String] val) =>
    """
    Begin partition migration
    """
    if _partition_routers.size() == 0 then
      //no steps have been migrated
      @printf[I32](("Resuming message processing immediately. No partitions " +
        "to migrate.\n").cstring())
      _resume_the_world(_worker_name)
    end
    for w in target_workers.values() do
      @printf[I32]("Migrating partitions to %s\n".cstring(), w.cstring())
    end
    for state_name in _partition_routers.keys() do
      _migrate_partition_steps(state_name, target_workers)
    end

  be begin_migration_of_all() =>
    """
    Begin partition migration of *all* local stateful partition steps,
    as part of a shrink-to-fit migration.
    """
    if _partition_routers.size() == 0 then
      //no steps have been migrated
      @printf[I32](("Resuming message processing immediately. No partitions " +
        "to migrate.\n").cstring())
      _resume_the_world(_worker_name)
    end

    let target_workers: Array[(String, OutgoingBoundary)] val =
      match _omni_router
      | let omr: OmniRouter =>
        omr.get_outgoing_boundaries_sorted()
      | None =>
        recover val Array[(String, OutgoingBoundary)] end
      end
    Invariant(target_workers.size() == _outgoing_boundaries.size())

    @printf[I32]("Migrating all partitions to %d remaining workers\n".cstring(),
      target_workers.size())
    for state_name in _partition_routers.keys() do
      _migrate_all_partition_steps(state_name, target_workers)
    end

  be step_migration_complete(step_id: StepId) =>
    """
    Step with provided step id has been created on another worker.
    """
    _step_waiting_list.unset(step_id)
    if (_step_waiting_list.size() == 0) then
      if _leaving_in_process then
        _send_leaving_worker_done_migrating()
        _recovery_file_cleaner.clean_recovery_files()
      else
        _send_migration_batch_complete()
      end
    end

  fun _send_migration_batch_complete() =>
    """
    Inform migration target that the entire migration batch has been sent.
    """
    @printf[I32]("--Sending migration batch complete msg to new workers\n"
      .cstring())
    for target in _migration_target_ack_list.values() do
      try
        _outgoing_boundaries(target)?.send_migration_batch_complete()
      else
        Fail()
      end
    end

  fun _send_leaving_worker_done_migrating() =>
    """
    Inform migration target that the entire migration batch has been sent.
    """
    @printf[I32]("--Sending leaving worker done migration msg to cluster\n"
      .cstring())
    try
      let msg = ChannelMsgEncoder.leaving_worker_done_migrating(_worker_name,
        _auth)?
      _connections.send_control_to_cluster(msg)
    else
      Fail()
    end

  be remote_mute_request(originating_worker: String) =>
    """
    A remote worker requests that we mute all sources and data channel.
    """
    _mute_request(originating_worker)

  fun ref _mute_request(originating_worker: String) =>
    _stopped_worker_waiting_list.set(originating_worker)
    _stop_all_local()

  be remote_unmute_request(originating_worker: String) =>
    """
    A remote worker requests that we unmute all sources and data channel.
    """
    _unmute_request(originating_worker)

  fun ref _unmute_request(originating_worker: String) =>
    if _stopped_worker_waiting_list.size() > 0 then
      _stopped_worker_waiting_list.unset(originating_worker)
      if (_stopped_worker_waiting_list.size() == 0) then
        if (_migration_target_ack_list.size() == 0) and
          (_leaving_workers_waiting_list.size() == 0)
        then
          _try_resume_the_world()
        else
          // We should only unmute ourselves once _migration_target_ack_list is
          // empty for grow and _leaving_workers_waiting_list is empty for
          // shrink
          Fail()
        end
      end
    end

  be report_status(code: ReportStatusCode) =>
    match code
    | BoundaryCountStatus =>
      @printf[I32]("RouterRegistry knows about %s boundaries\n"
        .cstring(), _outgoing_boundaries.size().string().cstring())
    end
    for source in _sources.values() do
      source.report_status(code)
    end
    for boundary in _outgoing_boundaries.values() do
      boundary.report_status(code)
    end

  be remote_request_in_flight_ack(originating_worker: String,
    upstream_request_id: RequestId, upstream_requester_id: StepId)
  =>
    _add_remote_in_flight_ack_request(originating_worker, upstream_request_id,
      upstream_requester_id)

  be remote_request_in_flight_resume_ack(originating_worker: String,
    in_flight_resume_ack_id: InFlightResumeAckId, request_id: RequestId,
    requester_id: StepId, leaving_workers: Array[String] val)
  =>
    if _in_flight_ack_waiter.request_in_flight_resume_ack(in_flight_resume_ack_id,
      request_id, requester_id, EmptyInFlightAckRequester,
      AckFinishedCompleteAction(_auth, _worker_name, originating_worker,
        request_id, _connections))
    then
      // We need to ensure that all steps have the correct OmniRouter before
      // we can propagate the in_flight_resume request. On the workers that
      // did not initiate stop the world, we wait to distribute the OmniRouter
      // until this point since we don't need it to be up-to-date everywhere
      // during migration.
      _distribute_omni_router()
      _request_in_flight_resume_acks(in_flight_resume_ack_id,
        leaving_workers)
    end

  be process_migrating_target_ack(target: String) =>
    """
    Called when we receive a migration batch ack from the new worker
    (i.e. migration target) indicating it's ready to receive data messages
    """
    @printf[I32]("--Processing migration batch complete ack from %s\n"
      .cstring(), target.cstring())
    _migration_target_ack_list.unset(target)

    if _migration_target_ack_list.size() == 0 then
      @printf[I32]("--All new workers have acked migration batch complete\n"
        .cstring(), target.cstring())
      _connections.request_cluster_unmute()
      _unmute_request(_worker_name)
    end

  fun ref _initiate_request_in_flight_acks(custom_action: CustomAction,
    excluded_workers: Array[String] val = recover Array[String] end)
  =>
    _in_flight_ack_waiter.initiate_request(_id, custom_action)
    _connections.request_in_flight_acks(_id, this, excluded_workers)
    _request_in_flight_acks(_id)

  fun ref _add_remote_in_flight_ack_request(originating_worker: String,
    upstream_request_id: RequestId, upstream_requester_id: StepId)
  =>
    if not _in_flight_ack_waiter.already_added_request(upstream_requester_id)
    then
      _in_flight_ack_waiter.add_new_request(upstream_requester_id,
        upstream_request_id where custom_action = AckFinishedAction(_auth,
          _worker_name, originating_worker, upstream_request_id, _connections))

      _request_in_flight_acks(upstream_requester_id)
    else
      try
        let in_flight_ack_msg =
          ChannelMsgEncoder.in_flight_ack(_worker_name, upstream_request_id,
            _auth)?
        _connections.send_control(originating_worker, in_flight_ack_msg)
      else
        Fail()
      end
    end

  fun ref _request_in_flight_acks(requester_id: StepId) =>
    """
    Get in flight acks from all local sources and steps
    """
    ifdef debug then
      @printf[I32](("RouterRegistry requesting in flight acks for %s local " +
        "sources and %s local steps/sinks.\n").cstring(),
        _sources.size().string().cstring(),
        _data_router.size().string().cstring())
    end

    if _has_local_target() then
      // Request for sources
      for source in _sources.values() do
        let request_id =
          _in_flight_ack_waiter.add_consumer_request(requester_id)
        source.request_in_flight_ack(request_id, _id, this)
      end
      // Request for local steps and sinks
      _data_router.request_in_flight_ack(requester_id, this,
        _in_flight_ack_waiter)
    else
      _in_flight_ack_waiter.try_finish_in_flight_request_early(requester_id)
    end

  fun ref _request_in_flight_resume_acks(
    in_flight_resume_ack_id: InFlightResumeAckId,
    leaving_workers: Array[String] val)
  =>
    if _has_local_target() then
      // Request for sources
      for source in _sources.values() do
        let request_id = _in_flight_ack_waiter.add_consumer_resume_request()
        source.request_in_flight_resume_ack(in_flight_resume_ack_id,
          request_id, _id, this, leaving_workers)
      end
      // Request for local steps and sinks
      _data_router.request_in_flight_resume_ack(in_flight_resume_ack_id,
        _id, this, _in_flight_ack_waiter, leaving_workers)
    else
      _in_flight_ack_waiter.try_finish_resume_request_early()
    end

  fun _has_local_target(): Bool =>
    """
    Do we have at least one Source, Step, or Sink on this worker?
    """
    (_sources.size() > 0) or (_data_router.size() > 0)

  be add_connection_request_ids(r_ids: Array[RequestId] val) =>
    for r_id in r_ids.values() do
      _in_flight_ack_waiter.add_consumer_request(_id, r_id)
    end

  be add_connection_request_ids_for_complete(r_ids: Array[RequestId] val) =>
    for r_id in r_ids.values() do
      _in_flight_ack_waiter.add_consumer_resume_request(r_id)
    end

  be try_finish_in_flight_request_early(requester_id: StepId) =>
    _in_flight_ack_waiter.try_finish_in_flight_request_early(requester_id)

  be try_finish_resume_request_early() =>
    _in_flight_ack_waiter.try_finish_resume_request_early()

  be receive_in_flight_ack(request_id: RequestId) =>
    _in_flight_ack_waiter.unmark_consumer_request(request_id)

  be receive_in_flight_resume_ack(request_id: RequestId) =>
    _in_flight_ack_waiter.unmark_consumer_resume_request(request_id)

  fun _stop_all_local() =>
    """
    Mute all sources and data channel.
    """
    ifdef debug then
      @printf[I32]("RouterRegistry muting any local sources.\n".cstring())
    end
    for source in _sources.values() do
      match source
      | let tcp_source: TCPSource =>
        ifdef debug then
          @printf[I32]("_stop_all_local: stopping ?? source\n".cstring())
        end
        tcp_source.mute_source(_dummy_consumer)
      else
        @printf[I32]("_stop_all_local: skipping stop ?? source\n".cstring())
      end
    end

  fun _resume_all_local() =>
    """
    Unmute all sources and data channel.
    """
    ifdef debug then
      @printf[I32]("RouterRegistry unmuting any local sources.\n".cstring())
    end
    for source in _sources.values() do
      match source
      | let tcp_source: TCPSource =>
        ifdef debug then
          @printf[I32]("_resume_all_local: resuming ?? source\n".cstring())
        end
        tcp_source.unmute_source(_dummy_consumer)
      else
        @printf[I32]("_resume_all_local: skipping resume ?? source\n".cstring())
      end
    end

  fun _resume_all_remote() =>
    try
      let msg = ChannelMsgEncoder.resume_the_world(_worker_name, _auth)?
      _connections.send_control_to_cluster(msg)
    else
      Fail()
    end

  fun ref try_to_resume_processing_immediately() =>
    if _step_waiting_list.size() == 0 then
      _send_migration_batch_complete()
    end

  be ack_migration_batch_complete(sender_name: String) =>
    """
    Called when a new (joining) worker needs to ack to worker sender_name that
    it's ready to start receiving messages after migration
    """
    _connections.ack_migration_batch_complete(sender_name)

  fun ref _migrate_partition_steps(state_name: String,
    target_workers: Array[String] val)
  =>
    """
    Called to initiate migrating partition steps to a target worker in order
    to rebalance.
    """
    try
      for w in target_workers.values() do
        @printf[I32]("Migrating steps for %s partition to %s\n".cstring(),
          state_name.cstring(), w.cstring())
      end

      let sorted_target_workers =
        ArrayHelpers[String].sorted[String](target_workers)

      let tws = recover trn Array[(String, OutgoingBoundary)] end
      for w in sorted_target_workers.values() do
        let boundary = _outgoing_boundaries(w)?
        tws.push((w, boundary))
      end
      let partition_router = _partition_routers(state_name)?
      partition_router.rebalance_steps_grow(consume tws, _worker_count(),
        state_name, this)
    else
      Fail()
    end

  fun ref _migrate_all_partition_steps(state_name: String,
    target_workers: Array[(String, OutgoingBoundary)] val)
  =>
    """
    Called to initiate migrating all partition steps the set of remaining
    workers.
    """
    try
      @printf[I32]("Migrating steps for %s partition to %d workers\n"
        .cstring(), state_name.cstring(), target_workers.size())
      let partition_router = _partition_routers(state_name)?
      partition_router.rebalance_steps_shrink(target_workers, state_name, this)
    else
      Fail()
    end

  fun ref add_to_step_waiting_list(step_id: StepId) =>
    _step_waiting_list.set(step_id)

  /////////////////
  // Shrink to Fit
  /////////////////
  be initiate_shrink(remaining_workers: Array[String] val,
    leaving_workers: Array[String] val)
  =>
    """
    This should only be called on the worker contacted via an external
    message to initiate a shrink.
    """
    if ArrayHelpers[String].contains[String](leaving_workers, _worker_name)
    then
      // Since we're one of the leaving workers, we're handing off
      // responsibility for the shrink to one of the remaining workers.
      try
        let shrink_initiator = remaining_workers(0)?
        let msg = ChannelMsgEncoder.initiate_shrink(remaining_workers,
          leaving_workers, _auth)?
        _connections.send_control(shrink_initiator, msg)
      else
        Fail()
      end
    else
      @printf[I32]("~~~Initiating shrink~~~\n".cstring())
      @printf[I32]("-- Remaining workers: \n".cstring())
      for w in remaining_workers.values() do
        @printf[I32]("-- -- %s\n".cstring(), w.cstring())
      end

      @printf[I32]("-- Leaving workers: \n".cstring())
      for w in leaving_workers.values() do
        @printf[I32]("-- -- %s\n".cstring(), w.cstring())
      end
      _stop_the_world_in_process = true
      _initiated_stop_the_world = true
      _stop_the_world_for_shrink_migration()
      try
        let msg = ChannelMsgEncoder.prepare_shrink(remaining_workers,
          leaving_workers, _auth)?
        for w in remaining_workers.values() do
          _connections.send_control(w, msg)
        end
      else
        Fail()
      end
      _prepare_shrink(remaining_workers, leaving_workers)
      _initiate_request_in_flight_acks(LeavingMigrationAction(_auth,
        _worker_name, remaining_workers, leaving_workers, _connections))
    end

  be prepare_shrink(remaining_workers: Array[String] val,
    leaving_workers: Array[String] val)
  =>
    """
    One worker is contacted via external message to begin autoscale
    shrink. That worker then informs every other worker to prepare for
    shrink. This behavior is called in response to receiving that message
    from the contacted worker.
    """
    _prepare_shrink(remaining_workers, leaving_workers)

  fun ref _prepare_shrink(remaining_workers: Array[String] val,
    leaving_workers: Array[String] val)
  =>
    for w in leaving_workers.values() do
      _remove_worker_from_omni_router(w)
    end
    ifdef debug then
      Invariant(_leaving_workers_waiting_list.size() == 0)
    end
    if not _stop_the_world_in_process then
      _stop_the_world_in_process = true
      _stop_the_world_for_shrink_migration()
    end

    for w in leaving_workers.values() do
      _leaving_workers_waiting_list.set(w)
    end
    for (p_id, router) in _stateless_partition_routers.pairs() do
      let new_router = router.calculate_shrink(remaining_workers)
      _distribute_stateless_partition_router(new_router)
      _stateless_partition_routers(p_id) = new_router
    end

  be begin_leaving_migration(remaining_workers: Array[String] val,
    leaving_workers: Array[String] val)
  =>
    """
    This should only be called on a worker designated to leave the cluster
    as part of shrink to fit.
    """
    @printf[I32]("Beginning process of leaving cluster.\n".cstring())
    _leaving_in_process = true
    _leaving_workers = leaving_workers
    if _partition_routers.size() == 0 then
      //no steps have been migrated
      @printf[I32](("No partitions to migrate.\n").cstring())
      _send_leaving_worker_done_migrating()
      return
    end

    let sorted_remaining_workers =
      ArrayHelpers[String].sorted[String](remaining_workers)

    let rws_trn = recover trn Array[(String, OutgoingBoundary)] end
    for w in sorted_remaining_workers.values() do
      try
        let boundary = _outgoing_boundaries(w)?
        rws_trn.push((w, boundary))
      else
        Fail()
      end
    end
    let rws = consume val rws_trn
    if rws.size() == 0 then Fail() end

    @printf[I32]("Migrating all partitions to %d remaining workers\n"
      .cstring(), remaining_workers.size())
    for state_name in _partition_routers.keys() do
      _migrate_all_partition_steps(state_name, rws)
    end

  fun ref _stop_the_world_for_shrink_migration() =>
    """
    We currently stop all message processing before migrating partitions and
    updating routers/routes.
    """
    @printf[I32]("~~~Stopping message processing for leaving workers.~~~\n"
      .cstring())
    _mute_request(_worker_name)
    _connections.stop_the_world()

  be disconnect_from_leaving_worker(worker: String) =>
    _connections.disconnect_from(worker)
    try
      _remove_worker(worker)
      _outgoing_boundaries.remove(worker)?
      _outgoing_boundaries_builders.remove(worker)?
    else
      Fail()
    end

    _distribute_boundary_builders()
    if not _leaving_in_process then
      ifdef debug then
        Invariant(_leaving_workers_waiting_list.size() > 0)
      end
      _leaving_workers_waiting_list.unset(worker)
      if _leaving_workers_waiting_list.size() == 0 then
        _connections.request_cluster_unmute()
        _unmute_request(_worker_name)
      end
    end

  /////
  // Step moved off this worker or new step added to another worker
  /////
  fun ref move_stateful_step_to_proxy[K: (Hashable val & Equatable[K] val)](
    id: U128, step: Step, proxy_address: ProxyAddress, key: K,
    state_name: String)
  =>
    """
    Called when a stateful step has been migrated off this worker to another
    worker
    """
    _remove_all_routes_to_step(step)
    _add_state_proxy_to_partition_router[K](proxy_address, key, state_name)
    _move_step_to_proxy(id, proxy_address)

  fun ref _remove_all_routes_to_step(step: Step) =>
    for source in _sources.values() do
      source.remove_route_to_consumer(step)
    end
    _data_router.remove_routes_to_consumer(step)

  fun ref _move_step_to_proxy(id: U128, proxy_address: ProxyAddress) =>
    """
    Called when a step has been migrated off this worker to another worker
    """
    _remove_step_from_data_router(id)
    _add_proxy_to_omni_router(id, proxy_address)

  be add_state_proxy[K: (Hashable val & Equatable[K] val)](id: U128,
    proxy_address: ProxyAddress, key: K, state_name: String)
  =>
    """
    Called when a stateful step has been added to another worker
    """
    _add_state_proxy_to_partition_router[K](proxy_address, key, state_name)
    _add_proxy_to_omni_router(id, proxy_address)

  fun ref _add_state_proxy_to_partition_router[
    K: (Hashable val & Equatable[K] val)](proxy_address: ProxyAddress,
    key: K, state_name: String)
  =>
    try
      let proxy_router = ProxyRouter(_worker_name,
        _outgoing_boundaries(proxy_address.worker)?, proxy_address, _auth)
      let partition_router =
        _partition_routers(state_name)?.update_route[K](key, proxy_router)?
      _partition_routers(state_name) = partition_router
      _distribute_partition_router(partition_router)
    else
      Fail()
    end

  fun ref _remove_step_from_data_router(id: U128) =>
    try
      let moving_step = _data_router.step_for_id(id)?

      let new_data_router = _data_router.remove_route(id)
      _data_router = new_data_router
      _distribute_data_router()
    else
      Fail()
    end

  fun ref _add_proxy_to_omni_router(id: U128,
    proxy_address: ProxyAddress)
  =>
    match _omni_router
    | let o: OmniRouter =>
      _omni_router = o.update_route_to_proxy(id, proxy_address)
    else
      Fail()
    end

  /////
  // Step moved onto this worker
  /////
  be receive_immigrant_step(subpartition: StateSubpartition,
    runner_builder: RunnerBuilder, reporter: MetricsReporter iso,
    recovery_replayer: RecoveryReplayer, msg: StepMigrationMsg)
  =>
    let outgoing_boundaries = recover iso Map[String, OutgoingBoundary] end
    for (k, v) in _outgoing_boundaries.pairs() do
      outgoing_boundaries(k) = v
    end

    match _event_log
    | let event_log: EventLog =>
      match _omni_router
      | let omnr: OmniRouter =>
        let step = Step(_auth, runner_builder(where event_log = event_log,
          auth = _auth), consume reporter, msg.step_id(),
          runner_builder.route_builder(), event_log, recovery_replayer,
          consume outgoing_boundaries where omni_router = omnr)
        step.receive_state(msg.state())
        msg.update_router_registry(this, step)
      else
        Fail()
      end
    else
      Fail()
    end

  fun ref move_proxy_to_stateful_step[K: (Hashable val & Equatable[K] val)](
    id: U128, target: Consumer, key: K, state_name: String,
    source_worker: String)
  =>
    """
    Called when a stateful step has been migrated to this worker from another
    worker
    """
    try
      match target
      | let step: Step =>
        _register_omni_router_step(step)
        _data_router = _data_router.add_route(id, step)
        _distribute_data_router()
        let partition_router =
          _partition_routers(state_name)?.update_route[K](key, step)?
        _distribute_partition_router(partition_router)
        // Add routes to state computation targets to state step
        match _pre_state_data
        | let psds: Array[PreStateData] val =>
          for psd in psds.values() do
            if psd.state_name() == state_name then
              for tid in psd.target_ids().values() do
                try
                  let target_router =
                    DirectRouter(_data_router.step_for_id(tid)?)
                  step.register_routes(target_router,
                    psd.forward_route_builder())
                end
              end
            end
          end
        else
          Fail()
        end
        _partition_routers(state_name) = partition_router
      else
        Fail()
      end
    else
      Fail()
    end
    _move_proxy_to_step(id, target, source_worker)
    _connections.notify_cluster_of_new_stateful_step[K](id, key, state_name,
      recover [source_worker] end)

  fun ref _move_proxy_to_step(id: U128, target: Consumer,
    source_worker: String)
  =>
    """
    Called when a step has been migrated to this worker from another worker
    """
    match _omni_router
    | let o: OmniRouter =>
      _omni_router = o.update_route_to_step(id, target)
    else
      Fail()
    end

class MigrationAction is CustomAction
  let _registry: RouterRegistry
  let _target_workers: Array[String] val

  new iso create(registry: RouterRegistry, target_workers: Array[String] val)
  =>
    _registry = registry
    _target_workers = target_workers

  fun ref apply() =>
    _registry.begin_migration(_target_workers)

class LeavingMigrationAction is CustomAction
  let _auth: AmbientAuth
  let _worker_name: String
  let _remaining_workers: Array[String] val
  let _leaving_workers: Array[String] val
  let _connections: Connections

  new iso create(auth: AmbientAuth, worker_name: String,
    remaining_workers: Array[String] val, leaving_workers: Array[String] val,
    connections: Connections)
  =>
    _auth = auth
    _worker_name = worker_name
    _remaining_workers = remaining_workers
    _leaving_workers = leaving_workers
    _connections = connections

  fun ref apply() =>
    try
      let msg = ChannelMsgEncoder.begin_leaving_migration(_remaining_workers,
        _leaving_workers, _auth)?
      for w in _leaving_workers.values() do
        if w == _worker_name then
          // Leaving workers shouldn't be managing the shrink process.
          Fail()
        else
          _connections.send_control(w, msg)?
        end
      end
    else
      Fail()
    end

class AckFinishedAction is CustomAction
  let _auth: AmbientAuth
  let _worker: String
  let _originating_worker: String
  let _request_id: RequestId
  let _connections: Connections

  new iso create(auth: AmbientAuth, worker: String, originating_worker: String,
    request_id: RequestId, connections: Connections)
  =>
    _auth = auth
    _worker = worker
    _originating_worker = originating_worker
    _request_id = request_id
    _connections = connections

  fun apply() =>
    try
      let in_flight_ack_msg =
        ChannelMsgEncoder.in_flight_ack(_worker, _request_id, _auth)?
      _connections.send_control(_originating_worker, in_flight_ack_msg)
    else
      Fail()
    end

class AckFinishedCompleteAction is CustomAction
  let _auth: AmbientAuth
  let _worker: String
  let _originating_worker: String
  let _request_id: RequestId
  let _connections: Connections

  new iso create(auth: AmbientAuth, worker: String, originating_worker: String,
    request_id: RequestId, connections: Connections)
  =>
    _auth = auth
    _worker = worker
    _originating_worker = originating_worker
    _request_id = request_id
    _connections = connections

  fun apply() =>
    try
      let in_flight_ack_msg =
        ChannelMsgEncoder.in_flight_resume_ack(_worker, _request_id, _auth)?
      _connections.send_control(_originating_worker, in_flight_ack_msg)
    else
      Fail()
    end

class ResumeTheWorldAction is CustomAction
  let _registry: RouterRegistry ref

  new create(registry: RouterRegistry ref) =>
    _registry = registry

  fun ref apply() =>
    _registry.initiate_resume_the_world()

class LogRotationAction is CustomAction
  let _registry: RouterRegistry

  new iso create(registry: RouterRegistry) =>
    _registry = registry

  fun ref apply() =>
    _registry.begin_log_rotation()

// TODO: Replace using this with the badly named SetIs once we address a bug
// in SetIs where unsetting doesn't reduce set size for type SetIs[String].
// This bug does not exist on the latest Pony compiler, only our fork.
class _StringSet
  let _map: Map[String, String] = _map.create()

  fun ref set(s: String) =>
    _map(s) = s

  fun ref unset(s: String) =>
    try _map.remove(s)? end

  fun ref clear() =>
    _map.clear()

  fun size(): USize =>
    _map.size()

  fun values(): MapValues[String, String, HashEq[String],
    this->HashMap[String, String, HashEq[String]]]^
  =>
    _map.values()
