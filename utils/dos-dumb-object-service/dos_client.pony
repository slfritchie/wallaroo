
use "buffered" // DEBUG for embedded AsyncJournal
use "collections"
use "files"
use "net"
use "promises"
use "time"

actor Main
  let _auth: (AmbientAuth | None)
  let _args: Array[String] val
  var _journal_path: String = "/dev/bogus"
  var _journal: (SimpleJournal2 | None) = None

  new create(env: Env) =>
    _auth = env.root
    _args = env.args
    try
/***** This code is racy but it works for prototyping/exploration purposes:
        _dos = DOSclient(env.out, env.root as AmbientAuth, "localhost", "9999")
      let dos = _dos as DOSclient

      @usleep[None](U32(100_000))

      let p0 = Promise[DOSreply]
      p0.next[None](
        {(a) =>
          env.out.print("PROMISE: I got array of size " + a.size().string())
          try
            for (file, size, appending) in (a as DOSreplyLS).values() do
              env.out.print("\t" + file + "," + size.string() + "," + appending.string())
            end
          end
        },
        {() => env.out.print("PROMISE: 1 BUMMER!")}
      )
      dos.do_ls(p0)

      let got_a_chunk = {(offset: USize, chunk: DOSreply): Bool =>
          ifdef "verbose" then
            None // try @printf[(">>>%s<<<\n".cstring(), (chunk as String).cstring()) end
          end
          true
        }
      let failed_a_chunk = {(offset: USize): Bool =>
           false
        }

      let offset0: USize = 0
      let p0b = Promise[DOSreply]
      // Fulfill needs an iso, so we can't use got_a_chunk directly as an arg.
      p0b.next[Bool](
        {(chunk: DOSreply): Bool =>
          got_a_chunk.apply(offset0, chunk)
        },
        {(): Bool => failed_a_chunk.apply(offset0) })
      dos.do_get_chunk("bar", offset0, 0, p0b)

      let path1 = "bar"
      let notify_get_file_complete = recover val
        {(success: Bool, bs: Array[Bool] val): None =>
        env.out.print("PROMISE: file transfer for " + path1 + " was success " + success.string() + " num_chunks " + bs.size().string())
        }
      end
      dos.do_get_file[Bool](path1, 47, 10, got_a_chunk, failed_a_chunk, notify_get_file_complete)

      env.out.print("BEFORE SLEEP 1")
      @usleep[None](U32(100_000))
      env.out.print("AFTER SLEEP 1")
      let p1 = Promise[DOSreply]
      p1.next[None](
        {(a) =>
          env.out.print("PROMISE: I got array of size " + a.size().string())
          try
            for (file, size, appending) in (a as DOSreplyLS).values() do
              env.out.print("\t" + file + "," + size.string() + "," + appending.string())
            end
          end
        },
        {() => env.out.print("PROMISE: 2 BUMMER!")}
      )
      dos.do_ls(p1)
      env.out.print("BEFORE 3 second sleep")
      @usleep[None](U32(3_100_000))
      dos.dispose()

 *****/

      let make_dos = recover val
        {(): DOSclient ? =>
          DOSclient(env.out, env.root as AmbientAuth, "localhost", "9999")
        } end

      _journal_path = _args(1)? + ".journal"
      let journal_fp = FilePath(_auth as AmbientAuth, _journal_path)?
      let rjc = RemoteJournalClient(_auth as AmbientAuth,
        journal_fp, _journal_path, make_dos, make_dos()?)
      let j_remote = recover iso SimpleJournalBackendRemote(rjc) end

      let j_file = recover iso SimpleJournalBackendLocalFile(journal_fp) end
      _journal = SimpleJournal2(consume j_file, consume j_remote)

      stage10()
    else
      Fail()
    end

/**********************************************************/

  be stage10() =>
    try
      _stage10(_journal as SimpleJournal2)
    else
      Fail()
    end

  fun _stage10(j: SimpleJournal2) =>
    // @usleep[None](U32(11_000))
    let ts = Timers
    /////////////////// let t = Timer(ScribbleSome(j, 20), 0, 50_000_000)
    let t = Timer(ScribbleSome(j, 50), 0, 2_000_000)
    ts(consume t)
    let tick = recover Tick.create(j) end
    let t2 = Timer(consume tick, 0, 5_000_000)
    ts(consume t2)
    @printf[I32]("STAGE 10: done\n".cstring())

class ScribbleSome is TimerNotify
  let _j: SimpleJournal2
  let _limit: USize
  var _c: USize = 0

  new iso create(j: SimpleJournal2, limit: USize) =>
    _j = j
    _limit = limit

  fun ref apply(t: Timer, c: U64): Bool =>
    let abc = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

    if _c >= _limit then
      @printf[I32]("TIMER: counter limit at %d, stopping\n".cstring(), _limit)
      false
    else
      @printf[I32]("TIMER: counter %d\n".cstring(), _c)
      let goo = recover val [_c.string() + "..." + abc] end
      _j.writev("some/file", goo)
      _c = _c + 1 // Bah, the 'c' arg counter is always 1
      true
    end

class DoLater is TimerNotify
  let _f: {(): Bool} iso

  new iso create(f: {(): Bool} iso) =>
    _f = consume f

  fun ref apply(t: Timer, c: U64): Bool =>
    _f()

class Tick is TimerNotify
  var _c: USize = 0
  let _j: SimpleJournal2

  new create(j: SimpleJournal2) =>
    _j = j

  fun ref apply(t: Timer, c: U64): Bool =>
    @printf[I32]("************************ Tick %d\n".cstring(), _c)
    _c = _c + 1
    if _c > 70 then
      _j.dispose_journal()
      false
    else
      true
    end

/**********************************************************/

actor RemoteJournalClient
  var _state: _RJCstate = _SLocalSizeDiscovery
  // TODO not sure which vars we really need
  let _auth: AmbientAuth
  let _journal_fp: FilePath
  let _journal_path: String
  let _make_dos: {(): DOSclient ?} val
  var _dos: DOSclient
  var _connected: Bool = false
  var _appending: Bool = false
  var _in_sync: Bool = false
  var _local_size: USize = 0
  var _remote_size: USize = 0
  var _disposed: Bool = false
  var _buffer: Array[(USize, ByteSeqIter, USize)] = _buffer.create()
  var _buffer_size: USize = 0
  // Beware of races if we ever change _buffer_max_size dynamically
  let _buffer_max_size: USize = 200 // (4*1024*1024)
  let _timers: Timers = Timers
  var _remote_size_discovery_sleep: USize = 1_000_000
  let _remote_size_discovery_max_sleep: USize = 1_000_000_000

  new create(auth: AmbientAuth, journal_fp: FilePath, journal_path: String,
    make_dos: {(): DOSclient ?} val, initial_dos: DOSclient)
  =>
    _auth = auth
    _journal_fp = journal_fp
    _journal_path = journal_path
    _make_dos = make_dos
    _dos = initial_dos
    @printf[I32]("RemoteJournalClient (last _state=%d): create\n".cstring(), _state.num())
    _set_connection_status_notifier()
    _local_size_discovery()

  be dispose() =>
    _dos.dispose()
    _connected = false
    _appending = false
    _in_sync = false
    _disposed = true

  fun ref _set_connection_status_notifier() =>
    let rjc = recover tag this end

    _dos.connection_status_notifier(recover iso
      {(connected: Bool): None => 
        @printf[I32]("RemoteJournalClient: lambda notifier = %s\n".cstring(), connected.string().cstring())
        rjc.dos_client_connection_status(connected)
      } end)

  fun ref _make_new_dos_then_local_size_discovery() =>
    @printf[I32]("RemoteJournalClient (last _state=%d):: _make_new_dos_then_local_size_discovery\n\n\n".cstring(), _state.num())
    _dos.dispose()
    _connected = false
    _appending = false
    _in_sync = false
    try
      _dos = _make_dos()?
      _set_connection_status_notifier()
    else
      Fail()
    end
    _local_size_discovery()

  be local_size_discovery() =>
    _local_size_discovery()

  fun ref _local_size_discovery() =>
    if _disposed then return end
    @printf[I32]("RemoteJournalClient (last _state=%d):: local_size_discovery for %s\n".cstring(), _state.num(),
      _journal_fp.path.cstring())
    _state = _SLocalSizeDiscovery

    if _appending then
      @printf[I32]("RemoteJournalClient (last _state=%d):: local_size_discovery _appending true\n".cstring(), _state.num())
      _make_new_dos_then_local_size_discovery()
    end
    _in_sync = false
    _remote_size_discovery_sleep = 1_000_000
    try
      _find_local_file_size()?
      _remote_size_discovery()
    else
      // We expect that this file will exist very very shortly.
      // Yield to the scheduler.
      local_size_discovery()
    end

  fun ref _find_local_file_size() ? =>
    let info = FileInfo(_journal_fp)?
    @printf[I32]("RemoteJournalClient: %s size %d\n".cstring(), _journal_fp.path.cstring(), info.size)
    _local_size = info.size

  fun ref _remote_size_discovery() =>
    if _disposed then return end
    @printf[I32]("RemoteJournalClient (last _state=%d):: remote_size_discovery for %s\n".cstring(), _state.num(), _journal_fp.path.cstring())
/***
    if _state.num() > _SRemoteSizeDiscovery.num() then
      // We have a race here with an async promise.  We are already
      // in a more advanced state, so ignore this message.
      @printf[I32]("RemoteJournalClient (last _state=%d):: remote_size_discovery, ignoring message for %s\n".cstring(), _state.num(), _journal_fp.path.cstring())
      return
    end
 ***/
    _state = _SRemoteSizeDiscovery

    let rsd = recover tag this end
    let p = Promise[DOSreply]
    // TODO add timeout goop
    p.next[None](
      {(a)(rsd) =>
        var remote_size: USize = 0 // Default if remote file doesn't exist
        try
          for (file, size, appending) in (a as DOSreplyLS).values() do
            if file == _journal_path then
              @printf[I32]("\tFound it\n".cstring())
              @printf[I32]("\t%s,%d,%s\n".cstring(), file.cstring(), size, appending.string().cstring())
              if appending then
                // Hmm, this is hopefully a benign race, with a prior
                // connection from us that hasn't been closed yet.
                // (If it's from another cluster member, that should
                //  never happen!)
                // So we stop iterating here and go back to the beginning
                // of the state machine; eventually the old session will
                // be closed by remote server and the file's appending
                // state will be false.
                rsd.remote_size_discovery_reply(false)
                return
              end              
              remote_size = size
              break
            end
          end
          rsd.remote_size_discovery_reply(true, remote_size)
        end
      },
      {()(rsd) =>
        @printf[I32]("PROMISE: remote_size_discovery BUMMER!\n".cstring())
        rsd.remote_size_discovery_reply(false)
      })
    _dos.do_ls(p)
    _remote_size_discovery_waiting()

  fun ref _remote_size_discovery_waiting() =>
    if _disposed then return end
    @printf[I32]("RemoteJournalClient (last _state=%d):: remote_size_discovery_waiting for %s\n".cstring(), _state.num(), _journal_fp.path.cstring())
/***
    if _state.num() != _SRemoteSizeDiscovery.num() then
      return
    end
 ***/
    _state = _SRemoteSizeDiscoveryWaiting

  be remote_size_discovery_reply(success: Bool, remote_size: USize = 0) =>
    if _disposed then return end
    @printf[I32]("RemoteJournalClient (last _state=%d):: remote_size_discovery_reply: success %s remote_size %d\n".cstring(), _state.num(), success.string().cstring(), remote_size)

/***    if _state.num() == _SRemoteSizeDiscoveryWaiting then ***/
      if success then
        _start_remote_file_append(remote_size)
      else
        _remote_size_discovery_sleep = _remote_size_discovery_sleep * 2
        _remote_size_discovery_sleep =
          _remote_size_discovery_sleep.min(_remote_size_discovery_max_sleep)
        let rsd = recover tag this end
        let later = DoLater(recover
          {(): Bool =>
            @printf[I32]("\n\t\t\t\tDoLater: remote_size_discovery after sleep_time %d\n".cstring(), _remote_size_discovery_sleep)
            rsd.remote_size_discovery_retry()
            false
          } end)
        let t = Timer(consume later, U64.from[USize](_remote_size_discovery_sleep))
        _timers(consume t)
        _state = _SRemoteSizeDiscovery
      end
/***    end ***/

  be remote_size_discovery_retry() =>
    @printf[I32]("RemoteJournalClient (last _state=%d):: remote_size_discovery_retry: \n".cstring(), _state.num())
    if _state.num() == _SRemoteSizeDiscovery.num() then
      _remote_size_discovery()
    end
/***
  be start_remote_file_append(remote_size: USize) =>
    _start_remote_file_append(remote_size)
 ***/
  fun ref _start_remote_file_append(remote_size: USize) =>
    if _disposed then return end
    @printf[I32]("RemoteJournalClient (last _state=%d):: start_remote_file_append for %s\n".cstring(), _state.num(), _journal_fp.path.cstring())
/***
    if _state.num() == _SStartRemoteFileAppend.num() then
      @printf[I32]("RemoteJournalClient (last _state=%d):: start_remote_file_append, ignoring message for %s\n".cstring(), _state.num(), _journal_fp.path.cstring())
      return
    elseif _state.num() > _SStartRemoteFileAppend.num() then
      @printf[I32]("RemoteJournalClient (last _state=%d):: start_remote_file_append TODO hey this seems to happen occasionally, is it truly bad or is ignoring it good enough?\n".cstring(), _state.num())
      return
    end
 ***/
    _state = _SStartRemoteFileAppend

    _remote_size = remote_size
    @printf[I32]("RemoteJournalClient: start_remote_file_append _local_size %d _remote_size %d\n".cstring(), _local_size, _remote_size)

/***
    if not _connected then
      @printf[I32]("RemoteJournalClient (last _state=%d):: start_remote_file_append not _connected line %d\n".cstring(), _state.num(), __loc.line())
      _make_new_dos_then_local_size_discovery()
      return
    end
 ***/

    let rsd = recover tag this end
    let p = Promise[DOSreply]
    // TODO add timeout goop
    p.next[None](
      {(reply)(rsd) =>
        try
          @printf[I32]("RemoteJournalClient: start_remote_file_append RES %s\n".cstring(), (reply as String).cstring())
          if (reply as String) == "ok" then
            rsd.start_remote_file_append_reply(true)
          else
            try @printf[I32]("RemoteJournalClient: start_remote_file_append failure (reason = %s), pause & looping TODO\n".cstring(), (reply as String).cstring()) else @printf[I32]("RemoteJournalClient: start_remote_file_append failure (reason = NOT-A-STRING), pause & looping TODO\n".cstring()) end
            rsd.start_remote_file_append_reply(false)
          end
        else
          Fail()
        end
      },
      {() =>
        @printf[I32]("RemoteJournalClient: start_remote_file_append REJECTED\n".cstring())
        rsd.start_remote_file_append_reply(false)
      }
    )
    _dos.start_streaming_append(_journal_path, _remote_size, p)
    _start_remote_file_append_waiting()

  be start_remote_file_append_waiting() =>
    _start_remote_file_append_waiting()

  fun ref _start_remote_file_append_waiting() =>
    if _disposed then return end
    @printf[I32]("RemoteJournalClient (last _state=%d):: start_remote_file_append_waiting for %s\n".cstring(), _state.num(), _journal_fp.path.cstring())
/***
    if _state.num() != _SStartRemoteFileAppend.num() then
      return
    end
 ***/
    _state = _SStartRemoteFileAppendWaiting

  be start_remote_file_append_reply(success: Bool) =>
    if _disposed then return end
    @printf[I32]("RemoteJournalClient (last _state=%d):: start_remote_file_append_reply: success %s\n".cstring(), _state.num(), success.string().cstring())
    if _state.num() == _SStartRemoteFileAppendWaiting.num() then
      if success then
        _catch_up_state()
      else
        _local_size_discovery()
      end
    end

  fun ref _catch_up_state() =>
    if _disposed then return end
    @printf[I32]("RemoteJournalClient (last _state=%d):: catch_up_state _local_size %d _remote_size %d\n".cstring(), _state.num(), _local_size, _remote_size)
/***
    if _state.num() > _SCatchUp.num() then
      Fail()
    end
    if _state.num() < _SRemoteSizeDiscovery.num() then
      @printf[I32]("RemoteJournalClient (last _state=%d):: catch_up_state wrong state, returning\n".cstring(), _state.num())
      return
    end
    if not _appending then
      @printf[I32]("RemoteJournalClient (last _state=%d):: catch_up_state not _appending, returning\n".cstring(), _state.num())
      return
    end
 ***/
    _state = _SCatchUp

/***
    if not _connected then
      @printf[I32]("RemoteJournalClient (last _state=%d):: catch_up_state not _connected line %d\n".cstring(), _state.num(), __loc.line())
      _make_new_dos_then_local_size_discovery()
      return
    end
***/
    if _local_size == _remote_size then
      @printf[I32]("RemoteJournalClient (last _state=%d):: catch_up_state line %d\n".cstring(), _state.num(), __loc.line())
      send_buffer_state()
    elseif _local_size > _remote_size then
      @printf[I32]("RemoteJournalClient (last _state=%d):: catch_up_state line %d\n".cstring(), _state.num(), __loc.line())
      _catch_up_send_block()
    else
      @printf[I32]("RemoteJournalClient (last _state=%d):: catch_up_state line %d\n".cstring(), _state.num(), __loc.line())
      Fail()
    end

  fun ref _catch_up_send_block() =>
    let missing_bytes = _local_size - _remote_size
    //let block_size = missing_bytes.min(1024*1024)
    let block_size = missing_bytes.min(50)

    @printf[I32]("\t_catch_up_send_block: block_size = %d\n".cstring(), block_size)
    with file = File.open(_journal_fp) do
      file.seek(_remote_size.isize())
      let bytes = recover val file.read(block_size) end
      let goo = recover val [bytes] end
      @printf[I32]("\t_catch_up_send_block: _remote_size %d bytes size = %d\n".cstring(), _remote_size, bytes.size())
      _dos.send_unframed(goo)
      _remote_size = _remote_size + bytes.size()
      _catch_up_state()
    end

  be send_buffer_state() =>
    _send_buffer_state()

  fun ref _send_buffer_state() =>
    if _disposed then return end
    @printf[I32]("RemoteJournalClient (last _state=%d):: send_buffer_state _local_size %d _remote_size %d\n".cstring(), _state.num(), _local_size, _remote_size)
    if _state.num() != _SCatchUp.num() then
      return
    end
    if not _appending then
      @printf[I32]("RemoteJournalClient (last _state=%d):: send_buffer_state not _appending, returning\n".cstring(), _state.num())
      return
    end
    if _buffer_size > _buffer_max_size then
      // We know the size of the remote file: we have been through
      // 0 or more catch-up cycles.  So, let's update our knowledge of
      // the local file size, then go through another catch-up cycle.
      try
        _find_local_file_size()?
        _buffer.clear()
        _buffer_size = 0
      else
        Fail()
      end
      _catch_up_state()
      return
    end
    _state = _SSendBuffer

    if _buffer_size == 0 then
      in_sync_state()
    else
      for (offset, data, data_size) in _buffer.values() do
        if (offset + data_size) <= _remote_size then
          @printf[I32]("\t======send_buffer_state: skipping offset %d data_size %d remote_size %d\n".cstring(), offset, data_size, _remote_size)
          None
        elseif (offset < _remote_size) and ((offset + data_size) > _remote_size) then
          // IIRC POSIX says that we shouldn't have to worry about this
          // case *if* the local file writer is always unbuffered?
          // TODO But I probably don't remember correctly: what if
          // the local file writer's writev(2) call was a partial write?
          // Our local-vs-remote sync protocol then syncs to an offset
          // that ends at the partial write.  And then we stumble into
          // this case?
          //
          // This case is very rare but does happen.  TODO is it worth
          // splitting the data here? and procedding forward?
          @printf[I32]("\t======send_buffer_state: TODO split offset %d data_size %d remote_size %d\n".cstring(), offset, data_size, _remote_size)
          _make_new_dos_then_local_size_discovery()
          return
        elseif offset == _remote_size then
          @printf[I32]("\t======send_buffer_state: send_unframed offset %d data_size %d remote_size %d\n".cstring(), offset, data_size, _remote_size)
          _local_size = _local_size + data_size // keep in sync with _remote_size
          _dos.send_unframed(data)
          _remote_size = _remote_size + data_size
        else
          Fail()
        end
      end
      _buffer.clear()
      _buffer_size = 0
      @printf[I32]("RemoteJournalClient (last _state=%d):: send_buffer_state _local_size %d _remote_size %d\n".cstring(), _state.num(), _local_size, _remote_size)
      in_sync_state()
    end

  fun /* Yes, fun, not be! */ ref in_sync_state() =>
    if _disposed then return end
    @printf[I32]("RemoteJournalClient (last _state=%d):: in_sync_state _local_size %d _remote_size %d\n".cstring(), _state.num(), _local_size, _remote_size)
    if _state.num() != _SSendBuffer.num() then
      Fail()
    end
    _state = _SInSync

    _in_sync = true

  be be_writev(offset: USize, data: ByteSeqIter, data_size: USize) =>
    if _disposed then return end
    @printf[I32]("RemoteJournalClient (last _state=%d):: be_writev offset %d data_size %d, _remote_size %d _buffer_size %d\n".cstring(), _state.num(), offset, data_size, _remote_size, _buffer_size)
    // TODO check offset sanity
    if _in_sync and (offset != (_remote_size + _buffer_size)) then
      if (offset + data_size) <= _remote_size then
        // During a catch-up phase, we copied a bunch of the missing
        // data from the local file -> remote file.  Due to asynchrony,
        // it's possible for the local file to be at size/offset B but
        // writev ops are delayed and only arrive here up to an
        // earlier offset A (where A < B).  The catchup phase has
        // already copied 100% of the bytes that are in this writev op.
        // We need to do nothing in this case.
        @printf[I32]("RemoteJournalClient (last _state=%d):: be_writev offset %d data_size %d, _remote_size %d hey got old/delayed writev op for offset %d data_size %d\n".cstring(), _state.num(), offset, data_size, _remote_size, offset, data_size)
        None
      else
        // WHOA, we have an out-of-order problem, or we're missing
        // a write, or something else terrible.
        // TODO Should we go back to local_size_discovery()?
        Fail()
      end
    else
      // We aren't in sync. Do nothing here and wait for offset sanity
      // checking at buffer flush.
      None
    end
    if _in_sync then
      _dos.send_unframed(data)
      _remote_size = _remote_size + data_size
    else
      if _buffer_size < _buffer_max_size then
        _buffer.push((offset, data, data_size))
      else
        // We're over the max size. Clear buffer if it's full, but
        // keep counting buffered bytes.
        if _buffer.size() > 0 then
          @printf[I32]("\t====be_writev: be_writev CLEAR _buffer\n".cstring())
          _buffer.clear()
        end
      end
      _buffer_size = _buffer_size + data_size
      @printf[I32]("\t====be_writev: be_writev new _buffer_size %d\n".cstring(), _buffer_size)
    end

  be dos_client_connection_status(connected: Bool) =>
    if _disposed then return end
    @printf[I32]("RemoteJournalClient (last _state=%d):: dos_client_connection_status %s\n".cstring(), _state.num(), connected.string().cstring())
    _connected = connected
    if not _connected then
      _appending = false
      _in_sync = false
    end
    if _connected then
      _local_size_discovery()
    end

/***
  be advise_state_change(state: _RJCstate, size: USize = 0,
    sleep_time: USize = 0, max_time: USize = 0, set_appending: Bool = false)
  =>
    @printf[I32]("RemoteJournalClient (last _state=%d):: advise_state_change %d\n".cstring(), _state.num(), state.num())
    match state
    | _SLocalSizeDiscovery =>
      if (_state.num() <= _SRemoteSizeDiscoveryWaiting.num())
        or (_state.num() == _SStartRemoteFileAppendWaiting.num()) then
        _local_size_discovery()
      end
    | _SRemoteSizeDiscovery =>
      if _state.num() == _SRemoteSizeDiscoveryWaiting.num() then
        _remote_size_discovery(sleep_time, max_time)
      end
    | _SStartRemoteFileAppend =>
      if _state.num() == _SRemoteSizeDiscoveryWaiting.num() then
        _start_remote_file_append(size)
      end
    | _SCatchUp =>
      if (_state.num() == _SStartRemoteFileAppendWaiting.num())
        or (_state.num() == _SCatchUp.num()) then
        if set_appending then
          _appending = true
        end
        _catch_up_state()
      end
    end
 ***/

type _RJCstate is
  (_SLocalSizeDiscovery | _SRemoteSizeDiscovery | _SRemoteSizeDiscoveryWaiting |
   _SStartRemoteFileAppend | _SStartRemoteFileAppendWaiting | _SCatchUp |
   _SSendBuffer | _SInSync)

primitive _SLocalSizeDiscovery
  fun num(): U8 => 0
primitive _SRemoteSizeDiscovery
  fun num(): U8 => 1
primitive _SRemoteSizeDiscoveryWaiting
  fun num(): U8 => 2
primitive _SStartRemoteFileAppend
  fun num(): U8 => 3
primitive _SStartRemoteFileAppendWaiting
  fun num(): U8 => 4
primitive _SCatchUp
  fun num(): U8 => 5
primitive _SSendBuffer
  fun num(): U8 => 6
primitive _SInSync
  fun num(): U8 => 7

/**********************************************************/

type DOSreplyLS is Array[(String, USize, Bool)] val
type DOSreply is (String val| DOSreplyLS val)
primitive DOSappend
primitive DOSgetchunk
primitive DOSls
primitive DOSnoop
type DOSop is (DOSappend | DOSgetchunk | DOSls | DOSnoop)

actor DOSclient
  let _out: OutStream
  var _auth: (AmbientAuth | None) = None
  let _host: String
  let _port: String
  var _sock: (TCPConnection | None) = None
  var _connected: Bool = false
  var _appending: Bool = false
  var _do_reconnect: Bool = true
  let _waiting_reply: Array[(DOSop, (Promise[DOSreply]| None))] = _waiting_reply.create()
  var _status_notifier: (({(Bool): None}) | None) = None

  new create(out: OutStream, auth: AmbientAuth, host: String, port: String) =>
    _out = out
    _auth = auth
    _host = host
    _port = port
    _reconn()

  be connection_status_notifier(status_notifier: {(Bool): None} iso) =>
    _status_notifier = consume status_notifier
    // Deal with a race where we were connected before the status notifier
    // lambda arrives here.
    if _connected then
      _call_status_notifier()
    end

  fun ref _reconn(): None =>
    ifdef "verbose" then
      @printf[I32]("DOS: calling _reconn\n".cstring())
    end
    _connected = false
    _appending = false
    try
      _sock = TCPConnection(_auth as AmbientAuth,
        recover DOSnotify(this, _out) end, _host, _port)
@printf[I32]("UUUGLY: _sock = 0x%lx\n".cstring(), _sock)
    end

  be dispose() =>
    ifdef "verbose" then
      @printf[I32]("DOS: &&&&&dispose\n".cstring())
    end
@printf[I32]("DOS: &&&&&dispose\n".cstring())
    _do_reconnect = false
    _dispose()

  fun ref _dispose() =>
    ifdef "verbose" then
      @printf[I32]("DOS: _dispose.  Promises to reject: %d\n".cstring(),
        _waiting_reply.size())
    end
@printf[I32]("DOS: _dispose.  Promises to reject: %d\n".cstring(),         _waiting_reply.size())
    try (_sock as TCPConnection).dispose() end
    _connected = false
    _appending = false
    for (op, p) in _waiting_reply.values() do
      match p
      | None => None
      | let pp: Promise[DOSreply] =>
        @printf[I32]("@@@@@@@@@@@@@@@@@@@@@@@@ promise reject, line %d\n".cstring(), __loc.line())
        pp.reject()
      end
    end
    _waiting_reply.clear()

  be connected(conn: TCPConnection) =>
@printf[I32]("DOS: connected\n".cstring())
@printf[I32]("UUUGLY: connected conn = 0x%lx\n".cstring(), conn)
    ifdef "verbose" then
      @printf[I32]("DOS: connected\n".cstring())
    end
    _connected = true
    _call_status_notifier()

  fun ref _call_status_notifier() =>
    try
      (_status_notifier as {(Bool): None})(true)
    end

  be disconnected(conn: TCPConnection) =>
@printf[I32]("DOS: disconnected\n".cstring())
@printf[I32]("UUUGLY: DISconnected conn = 0x%lx\n".cstring(), conn)
    ifdef "verbose" then
      @printf[I32]("DOS: disconnected\n".cstring())
    end
    _dispose()
    try
      (_status_notifier as {(Bool): None})(false)
    end
    if _do_reconnect then
      _reconn()
    end

  be send_unframed(data: ByteSeqIter) =>
    if _connected and _appending then
      try (_sock as TCPConnection).writev(data) end
    else
      @printf[I32]("\n\n\t\tsend_unframed: not (connected && appending)\n\n".cstring())
    end

  be start_streaming_append(filename: String, offset: USize,
    p: (Promise[DOSreply] | None) = None)
  =>
    let request: String iso = recover String end

    if _connected and (not _appending) then
      let pdu: String = "a" + filename + "\t" + offset.string()
      ifdef "verbose" then
        @printf[I32]("DOSc: start_streaming_append: %s offset %d\n".cstring(), filename.cstring(), offset)
      end
      request.push(0)
      request.push(0)
      request.push(0)
      request.push(pdu.size().u8()) // TODO: bogus if request size > 255
      request.append(pdu)
      try (_sock as TCPConnection).write(consume request) end
      _waiting_reply.push((DOSappend, p))
    else
      match p
      | None =>
        ifdef "verbose" then
          @printf[I32]("DOSclient: ERROR: streaming_append not connected, no promise!  TODO\n".cstring())
        end
        None
      | let pp: Promise[DOSreply] =>
        ifdef "verbose" then
          @printf[I32]("DOSclient: ERROR: streaming_append not connected, reject!  TODO\n".cstring())
        end
        @printf[I32]("@@@@@@@@@@@@@@@@@@@@@@@@ promise reject, line %d\n".cstring(), __loc.line())
        pp.reject()
      end
    end

  be do_ls(p: (Promise[DOSreply] | None) = None) =>
    let request: String iso = recover String end

    if _connected and (not _appending) then
      ifdef "verbose" then
        @printf[I32]("DOSc: do_ls\n".cstring())
      end
      request.push(0)
      request.push(0)
      request.push(0)
      request.push(1)
      request.append("l")

      try (_sock as TCPConnection).write(consume request) end
      _waiting_reply.push((DOSls, p))
    else
      match p
      | None =>
        ifdef "verbose" then
          @printf[I32]("DOSclient: ERROR: ls not connected, no promise!\n".cstring())
        end
        None
      | let pp: Promise[DOSreply] =>
        ifdef "verbose" then
          @printf[I32]("DOSclient: ERROR: ls not connected, reject!  TODO\n".cstring())
        end
        @printf[I32]("@@@@@@@@@@@@@@@@@@@@@@@@ promise reject, line %d\n".cstring(), __loc.line())
        pp.reject()
      end
    end

  be do_get_chunk(filename: String, offset: USize, size: USize,
    p: (Promise[DOSreply] | None) = None)
  =>
    let request: String iso = recover String end

    if _connected and (not _appending) then
      let pdu: String = "g" + filename + "\t" + offset.string() + "\t" + size.string()
      ifdef "verbose" then
        @printf[I32]("DOSc: do_get_chunk: %s\n".cstring(), pdu.cstring())
      end

      request.push(0)
      request.push(0)
      request.push(0)
      request.push(pdu.size().u8()) // TODO: bogus if request size > 255
      request.append(pdu)
      try (_sock as TCPConnection).write(consume request) end
      _waiting_reply.push((DOSgetchunk, p))
    else
      match p
      | None =>
        ifdef "verbose" then
          @printf[I32]("DOSclient: ERROR: get_chunk not connected, no promise!  TODO\n".cstring())
        end
        None
      | let pp: Promise[DOSreply] =>
        ifdef "verbose" then
          @printf[I32]("DOSclient: ERROR: get_chunk not connected, reject!  TODO\n".cstring())
        end
        @printf[I32]("@@@@@@@@@@@@@@@@@@@@@@@@ promise reject, line %d\n".cstring(), __loc.line())
        pp.reject()
      end
    end

  be do_get_file[T: Any #share](filename: String,
    file_size: USize, chunk_size: USize,
    chunk_success: {(USize, DOSreply): T} val, chunk_failed: {(USize): T} val,
    notify_get_file_complete: {(Bool, Array[T] val): None} val,
    error_on_failure: Bool = true)
  =>
    let chunk_ps: Array[Promise[T]] = chunk_ps.create()

    for offset in Range[USize](0, file_size, chunk_size) do
      let p0 = Promise[DOSreply]
      // TODO add timeout goop
      let p1 = p0.next[T](
        {(chunk: DOSreply): T =>
          chunk_success.apply(offset, chunk)
        },
        {(): T ? =>
          if error_on_failure then
            // Cascade the error through the rest of the promise chain.
            // The end-of-file lambda will get an empty array of Ts and
            // thus won't be able to figure out where the error happened.
            error
          else
            // Allow chunk_failed to created a T object that the
            // end-of-file lambda can examine in its array of Ts and
            // thus figure out where the error happened.
            chunk_failed.apply(offset)
          end
        })
      chunk_ps.push(p1)
      do_get_chunk(filename, offset, chunk_size, p0)
    end

    let p_all_chunks1 = Promises[T].join(chunk_ps.values())
    p_all_chunks1.next[None](
      {(ts: Array[T] val): None =>
        ifdef "verbose" then
          @printf[I32]("PROMISE BIG: yay\n".cstring())
        end
        notify_get_file_complete(true, ts)
        },
      {(): None =>
        @printf[I32]("PROMISE BIG: BOOOOOO\n".cstring())
        let empty_array: Array[T] val = recover empty_array.create() end
       notify_get_file_complete(false, empty_array)
      })

  // Used only by the DOSnotify socket thingie
  be response(data: Array[U8] iso) =>
    let str = String.from_array(consume data)
    ifdef "verbose" then
      @printf[I32]("DOSclient GOT: %s\n".cstring(), str.cstring())
    end
    try
      (let op, let p) = _waiting_reply.shift()?
      match p
      | None =>
        None
      | let pp: Promise[DOSreply] =>
        try
          match op
          | DOSappend =>
            if str == "ok" then
              _appending = true
            end
            pp(str)
          | DOSls =>
            @printf[I32]("DOSclient ls response GOT: %s\n".cstring(), str.cstring())
            let lines = recover val str.split("\n") end
            let res: Array[(String, USize, Bool)] iso = recover res.create() end

            for l in lines.values() do
              let fs = l.split("\t")
              if fs.size() == 0 then
                // This is the split value after the final \n of the output
                break
              end
              let file = fs(0)?
              let size = fs(1)?.usize()?
              let b = if fs(2)? == "no" then false else true end
              res.push((file, size, b))
            end
            pp(consume res)
          | DOSgetchunk =>
            pp(str)
          end
        else
          // Protocol parsing error, e.g., for DOSls.
          // Reject so client can proceed.
          @printf[I32]("@@@@@@@@@@@@@@@@@@@@@@@@ promise reject, line %d\n".cstring(), __loc.line())
          pp.reject()
        end
      end
    else
      // If the error is due to the socket being closed, then
      // all promises that are still in_waiting_reply will
      // be rejected by our disconnected() behavior.
      None
    end

class DOSnotify is TCPConnectionNotify
  let _client: DOSclient
  let _out: OutStream
  var _header: Bool = true
  let _qqq_crashme: I64 = 15
  var _qqq_count: I64 = _qqq_crashme

  new create(client: DOSclient, out: OutStream) =>
    _client = client
    _out = out

  fun ref connect_failed(conn: TCPConnection ref) =>
    ifdef "verbose" then
      @printf[I32]("SOCK: connect_failed\n".cstring())
    end

  fun ref connected(conn: TCPConnection ref) =>
    ifdef "verbose" then
      @printf[I32]("SOCK: I am connected.\n".cstring())
    end
    _header = true
    conn.set_nodelay(true)
    conn.expect(4)
    _client.connected(conn)

  fun ref received(
    conn: TCPConnection ref,
    data: Array[U8] iso,
    times: USize)
    : Bool
  =>
    if _header then
      ifdef "verbose" then
        @printf[I32]("SOCK: received header\n".cstring())
      end
      try
        let expect = Bytes.to_u32(data(0)?, data(1)?, data(2)?, data(3)?).usize()
        conn.expect(expect)
        _header = false
      else
        ifdef "verbose" then
          @printf[I32]("Error reading header on control channel\n".cstring())
        end
      end
    else
      ifdef "verbose" then
        @printf[I32]("SOCK: received payload\n".cstring())
      end
      _client.response(consume data)
      conn.expect(4)
      _header = true
    end
    false

  // NOTE: There is *not* a 1-1 correspondence between socket write/writev
  // and calling sent().  TCPConnection will do its own buffering and
  // may call sent() more or less frequently.
  fun ref sent(conn: TCPConnection ref, data: ByteSeq): ByteSeq =>
    ifdef "verbose" then
      @printf[I32]("SOCK: sent\n".cstring())
    end
    _qqq_count = _qqq_count - 1
    @printf[I32]("SOCK: sent @ crashme %d conn 0x%lx size %d\n".cstring(), _qqq_count, conn, data.size())
    if _qqq_count <= 0 then
      conn.close()
      conn.dispose()
      _qqq_count = _qqq_crashme
    end
    data

  fun ref sentv(conn: TCPConnection ref, data: ByteSeqIter): ByteSeqIter =>
    ifdef "verbose" then
      @printf[I32]("SOCK: sentv\n".cstring())
    end
    _qqq_count = _qqq_count - 1
    @printf[I32]("SOCK: sentv @ crashme %d conn 0x%lx size %d\n".cstring(), _qqq_count, conn, I32(-6))
    if _qqq_count <= 0 then
      conn.close()
      conn.dispose()
      _qqq_count = _qqq_crashme
    end
    data

  fun ref closed(conn: TCPConnection ref) =>
    ifdef "verbose" then
      @printf[I32]("SOCK: closed\n".cstring())
    end
    _client.disconnected(conn)

  fun ref throttled(conn: TCPConnection ref) =>
    ifdef "verbose" then
      @printf[I32]("SOCK: throttled\n".cstring())
    end
    @printf[I32]("SOCK: throttled, TODO\n".cstring())

  fun ref unthrottled(conn: TCPConnection ref) =>
    ifdef "verbose" then
      @printf[I32]("SOCK: unthrottled\n".cstring())
    end
    @printf[I32]("SOCK: unthrottled, TODO\n".cstring())

primitive Bytes
  fun to_u32(a: U8, b: U8, c: U8, d: U8): U32 =>
    (a.u32() << 24) or (b.u32() << 16) or (c.u32() << 8) or d.u32()

/****************************/
/* Copied from backend.pony */
/****************************/

class AsyncJournalledFile
  let _file_path: String
  let _file: File
  let _journal: SimpleJournal2
  let _auth: AmbientAuth
  var _offset: USize
  let _do_local_file_io: Bool
  var _tag: USize = 1

  new create(filepath: FilePath, journal: SimpleJournal2,
    auth: AmbientAuth, do_local_file_io: Bool)
  =>
    _file_path = filepath.path
    _file = if do_local_file_io then
      File(filepath)
    else
      try // Partial func hack
        File(FilePath(auth, "/dev/null")?)
      else
        Fail()
        File(filepath)
      end
    end
    _journal = journal
    _auth = auth
    _offset = 0
    _do_local_file_io = do_local_file_io

  fun ref datasync() =>
    // TODO journal!
    ifdef "journaldbg" then
      @printf[I32]("### Journal: datasync %s\n".cstring(), _file_path.cstring())
    end
    if _do_local_file_io then
      _file.datasync()
    end

  fun ref dispose() =>
    ifdef "journaldbg" then
      @printf[I32]("### Journal: dispose %s\n".cstring(), _file_path.cstring())
    end
    // Nothing (?) to do for the journal
    if _do_local_file_io then
      _file.dispose()
    end

  fun ref errno(): (FileOK val | FileError val | FileEOF val | 
    FileBadFileNumber val | FileExists val | FilePermissionDenied val)
  =>
    // TODO journal!  Perhaps fake a File* error if journal write failed?
    _file.errno()

  fun ref position(): USize val =>
    _file.position()

  fun ref print(data: (String val | Array[U8 val] val)): Bool val =>
    ifdef "journaldbg" then
      @printf[I32]("### Journal: print %s {data}\n".cstring(), _file_path.cstring())
    end
    _journal.writev(_file_path, [data; "\n"], _tag)
    _tag = _tag + 1

    if _do_local_file_io then
      _file.writev([data; "\n"])
    else
      true // TODO journal writev success/failure
    end

  fun ref read(len: USize): Array[U8 val] iso^ =>
    _file.read(len)

  fun ref seek_end(offset: USize): None =>
    // TODO journal!
    ifdef "journaldbg" then
      @printf[I32]("### Journal: seek_end %s offset %d\n".cstring(), _file_path.cstring(), offset)
    end
    if _do_local_file_io then
      _file.seek_end(offset)
      _offset = _file.position()
    end

  fun ref seek_start(offset: USize): None =>
    // TODO journal!
    ifdef "journaldbg" then
      @printf[I32]("### Journal: seek_start %s offset %d\n".cstring(), _file_path.cstring(), offset)
    end
    if _do_local_file_io then
      _file.seek_start(offset)
      _offset = _file.position()
    end

  fun ref set_length(len: USize): Bool val =>
    ifdef "journaldbg" then
      @printf[I32]("### Journal: set_length %s len %d\n".cstring(), _file_path.cstring(), len)
    end
    _journal.set_length(_file_path, len, _tag)
    _tag = _tag + 1

    if _do_local_file_io then
      _file.set_length(len)
    else
      true // TODO journal set_length success/failure
    end

  fun ref size(): USize val =>
    if _do_local_file_io then
      _file.size()
    else
      Fail(); 0
    end

  fun ref sync() =>
    // TODO journal!
    ifdef "journaldbg" then
      @printf[I32]("### Journal: sync %s\n".cstring(), _file_path.cstring())
    end
    if _do_local_file_io then
      _file.sync()
    end

  fun ref writev(data: ByteSeqIter val): Bool val =>
    ifdef "journaldbg" then
      @printf[I32]("### Journal: writev %s {data}\n".cstring(), _file_path.cstring())
    end
    _journal.writev(_file_path, data, _tag)
    _tag = _tag + 1

    if _do_local_file_io then
      let ret = _file.writev(data)
      _offset = _file.position()
      ret
    else
      true
    end

trait SimpleJournalAsyncResponseReceiver
  be async_io_ok(j: SimpleJournal2 tag, optag: USize)
  be async_io_error(j: SimpleJournal2 tag, optag: USize)

trait SimpleJournalBackend
  fun ref be_dispose(): None

  fun ref be_position(): USize

  fun ref be_writev(offset: USize,
    data: ByteSeqIter val, data_size: USize): Bool


class SimpleJournalBackendLocalFile is SimpleJournalBackend
  let _j_file: File

  new create(filepath: FilePath) =>
    _j_file = File(filepath)
    _j_file.seek_end(0)

  fun ref be_dispose() =>
    _j_file.dispose()

  fun ref be_position(): USize =>
    _j_file.position()

  fun ref be_writev(offset: USize, data: ByteSeqIter val, data_size: USize)
  : Bool
  =>
    let res1 = _j_file.writev(data)
    let res2 = _j_file.flush()
    if not (res1 and res2) then
      // TODO: The RemoteJournalClient assumes that data written to the
      // local journal file is always up-to-date and never buffered.
      Fail()
    end
    true

class SimpleJournalBackendRemote is SimpleJournalBackend
  let _rjc: RemoteJournalClient

  new create(rjc: RemoteJournalClient) =>
    _rjc = rjc

  fun ref be_dispose() =>
    _rjc.dispose()

  fun ref be_position(): USize =>
    666 // TODO

  fun ref be_writev(offset: USize, data: ByteSeqIter val, data_size: USize)
  : Bool
  =>
    // TODO offset sanity check
    // TODO offset update
    @printf[I32]("SimpleJournalBackendRemote: be_writev offset %d data_size %d\n".cstring(), offset, data_size)
    _rjc.be_writev(offset, data, data_size)
    true

actor SimpleJournal2
  var _j_file: SimpleJournalBackend
  var _j_remote: SimpleJournalBackend
  var _j_closed: Bool
  var _j_file_size: USize
  let _encode_io_ops: Bool
  let _owner: (None tag | SimpleJournalAsyncResponseReceiver tag)

  new create(j_file: SimpleJournalBackend iso,
    j_remote: SimpleJournalBackend iso,
    encode_io_ops: Bool = true,
    owner: (None tag | SimpleJournalAsyncResponseReceiver tag) = None)
  =>
    _encode_io_ops = encode_io_ops
    _owner = owner

    _j_file = consume j_file
    _j_remote = consume j_remote
    _j_closed = false
    _j_file_size = _j_file.be_position()

  // TODO This method only exists because of prototype hack laziness
  // that does not refactor both RotatingEventLog & SimpleJournal.
  // It is used only by RotatingEventLog.
  be dispose_journal() =>
    _j_file.be_dispose()
    _j_remote.be_dispose()
    _j_closed = true

  be set_length(path: String, len: USize, optag: USize = 0) =>
    if _j_closed then
      Fail()
    end
    if _encode_io_ops then
      (let pdu, let pdu_size) = _SJ.encode_request(optag, _SJ.set_length(),
        recover [len] end, recover [path] end)
      let wb: Writer = wb.create()

      if pdu_size > U32.max_value().usize() then
        Fail()
      end
      wb.u32_be(pdu_size.u32())
      wb.writev(consume pdu)
      let data_size = wb.size()
      let data = recover val wb.done() end
      let ret = _j_file.be_writev(_j_file_size, data, data_size)
      if ret then
        _j_remote.be_writev(_j_file_size, data, data_size)
        _j_file_size = _j_file_size + data_size
      else
        Fail() // TODO?
      end
      ret
    else
      Fail()
    end

  be writev(path: String, data: ByteSeqIter val, optag: USize = 0) =>
    if _j_closed then
      Fail()
    end
    let write_res =
      if _encode_io_ops then
        let bytes: Array[U8] trn = recover bytes.create() end
        let wb: Writer = wb.create()

          for bseq in data.values() do
            bytes.reserve(bseq.size())
            match bseq
            | let s: String =>
              // no: bytes.concat(s.values())
              for b in s.values() do
                bytes.push(b)
              end
            | let a: Array[U8] val =>
              // no: bytes.concat(a.values())
              for b in a.values() do
                bytes.push(b)
              end
            end
          end

        let bytes_size = bytes.size()
        (let pdu, let pdu_size) = _SJ.encode_request(optag, _SJ.writev(),
          [], [path; consume bytes])

        wb.u32_be(pdu_size.u32())
        wb.writev(consume pdu)
        ifdef "journaldbg" then
          @printf[I32]("### SimpleJournal: writev %s bytes %d pdu_size %d optag %d\n".cstring(), path.cstring(), bytes_size, pdu_size, optag)
        end
        let data2_size = wb.size()
        let data2 = recover val wb.done() end
        let ret = _j_file.be_writev(_j_file_size, data2, data2_size)
@printf[I32]("### SimpleJournal: writev %s bytes %d pdu_size %d optag %d RET %s\n".cstring(), path.cstring(), bytes_size, pdu_size, optag, ret.string().cstring())
        if ret then
          _j_remote.be_writev(_j_file_size, data2, data2_size)
          _j_file_size = _j_file_size + data2_size
        else
          Fail() // TODO?
        end
        ret
      else
        var data_size: USize = 0

        for d in data.values() do
          data_size = data_size + d.size()
        end
        let ret = _j_file.be_writev(_j_file_size, data, data_size)
        if ret then
          _j_remote.be_writev(_j_file_size, data, data_size)
          _j_file_size = _j_file_size + data_size
        else
          Fail() // TODO?
        end
        ret
      end
    if write_res then
      if optag > 0 then
        try
          let o = _owner as (SimpleJournalAsyncResponseReceiver tag)
          o.async_io_ok(this, optag)
        end
      end
    else
      // We don't know how many bytes were written.  ^_^
      // TODO So, I suppose we need to ask the OS about the file size
      // to figure that out so that we can do <TBD> to recover.
      try
        let o = _owner as (SimpleJournalAsyncResponseReceiver tag)
        o.async_io_error(this, optag)
      end
    end

/**********************************************************
|------+----------------+---------------------------------|
| Size | Type           | Description                     |
|------+----------------+---------------------------------|
|    1 | U8             | Protocol version = 0            |
|    1 | U8             | Op type                         |
|    1 | U8             | Number of int args              |
|    1 | U8             | Number of string/byte args      |
|    8 | USize          | Op tag                          |
|    8 | USize          | 1st int arg                     |
|    8 | USize          | nth int arg                     |
|    8 | USize          | Size of 1st string/byte arg     |
|    8 | USize          | Size of nth string/byte arg     |
|    X | String/ByteSeq | Contents of 1st string/byte arg |
|    X | String/ByteSeq | Contents of nth string/byte arg |
 **********************************************************/

primitive _SJ
  fun set_length(): U8 => 0
  fun writev(): U8 => 1

  fun encode_request(optag: USize, op: U8,
    ints: Array[USize], bss: Array[ByteSeq]):
    (Array[ByteSeq] iso^, USize)
  =>
    let wb: Writer = wb.create()

wb.write(">>>_SJ encode>>>")
    wb.u8(0)
    wb.u8(op)
    wb.u8(ints.size().u8())
    wb.u8(bss.size().u8())
    wb.u64_be(optag.u64())
    for i in ints.values() do
      wb.i64_be(i.i64())
    end
    for bs in bss.values() do
      wb.i64_be(bs.size().i64())
    end
    for bs in bss.values() do
      wb.write(bs)
    end
wb.write("<<<<<<")
    let size = wb.size()
    @printf[I32]("_SJ: encode_request size %d\n".cstring(), size)
    (wb.done(), size)

/*************************/
/* Copied from mort.pony */
/*************************/

primitive Fail
  """
  'This should never happen' error encountered. Bail out of our running
  program. Print where the error happened and exit.
  """
  fun apply(loc: SourceLoc = __loc) =>
    @fprintf[I32](
      @pony_os_stderr[Pointer[U8]](),
      "This should never happen: failure in %s at line %s\n".cstring(),
      loc.file().cstring(),
      loc.line().string().cstring())
    @exit[None](U8(1))
