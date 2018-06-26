use "files"
use "promises"
use "time"

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
  let _timeout_nanos: U64 = 2_000_000_000

  new create(auth: AmbientAuth, journal_fp: FilePath, journal_path: String,
    make_dos: {(): DOSclient ?} val, initial_dos: DOSclient)
  =>
    _auth = auth
    _journal_fp = journal_fp
    _journal_path = journal_path
    _make_dos = make_dos
    _dos = initial_dos
    _D.d8("RemoteJournalClient (last _state=%d): create\n", _state.num())
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
        _D.ds("RemoteJournalClient: lambda connected %s\n", connected.string())
        rjc.dos_client_connection_status(connected)
      } end)

  fun ref _make_new_dos_then_local_size_discovery() =>
    _D.d8("RemoteJournalClient (last _state=%d):: " +
      "_make_new_dos_then_local_size_discovery\n\n\n", _state.num())
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
      _D.d8("RemoteJournalClient (last _state=%d):: " +
        "local_size_discovery\n", _state.num())
    _state = _SLocalSizeDiscovery

    if _appending then
      _D.d8("RemoteJournalClient (last _state=%d):: " +
        "local_size_discovery _appending true\n", _state.num())
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
    _D.dsa("RemoteJournalClient: %s size %d\n", _journal_fp.path, info.size)
    _local_size = info.size

  fun ref _remote_size_discovery() =>
    if _disposed then return end
    _D.d8("RemoteJournalClient (last _state=%d):: remote_size_discovery\n",
      _state.num())
    _state = _SRemoteSizeDiscovery

    if not _connected then
      remote_size_discovery_reply(false)
      return
    end
    let rsd = recover tag this end
    let p = Promise[DOSreply]
    // TODO add timeout goop
    p.next[None](
      {(a)(rsd) =>
        var remote_size: USize = 0 // Default if remote file doesn't exist
        try
          for (file, size, appending) in (a as DOSreplyLS).values() do
            if file == _journal_path then
              _D.d("\tFound it\n")
              _D.ds6s("\t%s,%d,%s\n", file, size, appending.string())
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
        _D.d("PROMISE: remote_size_discovery BUMMER!\n")
        rsd.remote_size_discovery_reply(false)
      }).timeout(_timeout_nanos)
    _dos.do_ls(p)
    _remote_size_discovery_waiting()

  fun ref _remote_size_discovery_waiting() =>
    if _disposed then return end
    _D.d8("RemoteJournalClient (last _state=%d):: " +
      "remote_size_discovery_waiting", _state.num())
    _state = _SRemoteSizeDiscoveryWaiting

  be remote_size_discovery_reply(success: Bool, remote_size: USize = 0) =>
    if _disposed then return end
    _D.d8s6("RemoteJournalClient (last _state=%d):: " +
      "remote_size_discovery_reply: success %s remote_size %d\n",
      _state.num(), success.string(), remote_size)

    if _state.num() == _SRemoteSizeDiscoveryWaiting.num() then
      if success then
        _start_remote_file_append(remote_size)
      else
        _remote_size_discovery_sleep = _remote_size_discovery_sleep * 2
        _remote_size_discovery_sleep =
          _remote_size_discovery_sleep.min(_remote_size_discovery_max_sleep)
        let rsd = recover tag this end
        let later = DoLater(recover
          {(): Bool =>
            _D.d6("\n\t\t\t\tDoLater: remote_size_discovery " +
              " after sleep_time %d\n", _remote_size_discovery_sleep)
            rsd.remote_size_discovery_retry()
            false
          } end)
        let t = Timer(consume later, U64.from[USize](_remote_size_discovery_sleep))
        _timers(consume t)
        _state = _SRemoteSizeDiscovery
      end
    end

  be remote_size_discovery_retry() =>
    _D.d8("RemoteJournalClient (last _state=%d):: " + 
      "remote_size_discovery_retry: \n", _state.num())
    if _state.num() == _SRemoteSizeDiscovery.num() then
      _remote_size_discovery()
    end

  fun ref _start_remote_file_append(remote_size: USize) =>
    if _disposed then return end
    _D.d8("RemoteJournalClient (last _state=%d):: " +
      "start_remote_file_append\n", _state.num())
    _state = _SStartRemoteFileAppend

    _remote_size = remote_size
    _D.d66("RemoteJournalClient: start_remote_file_append " +
      "_local_size %d _remote_size %d\n", _local_size, _remote_size)

    if not _connected then
      _local_size_discovery()
      return
    end

    let rsd = recover tag this end
    let p = Promise[DOSreply]
    // TODO add timeout goop
    p.next[None](
      {(reply)(rsd) =>
        try
          _D.ds("RemoteJournalClient: start_remote_file_append RES %s\n",
            (reply as String))
          if (reply as String) == "ok" then
            rsd.start_remote_file_append_reply(true)
          else
            try
              _D.ds("RemoteJournalClient: start_remote_file_append failure " +
                "(reason = %s), pause & looping TODO\n", (reply as String))
            end
           rsd.start_remote_file_append_reply(false)
          end
        else
          Fail()
        end
      },
      {() =>
        _D.d("RemoteJournalClient: start_remote_file_append REJECTED\n")
        rsd.start_remote_file_append_reply(false)
      }
    ).timeout(_timeout_nanos)
    _dos.start_streaming_append(_journal_path, _remote_size, p)
    _start_remote_file_append_waiting()

  be start_remote_file_append_waiting() =>
    _start_remote_file_append_waiting()

  fun ref _start_remote_file_append_waiting() =>
    if _disposed then return end
    _D.d8("RemoteJournalClient (last _state=%d):: " +
      "start_remote_file_append_waiting\n", _state.num())
    _state = _SStartRemoteFileAppendWaiting

  be start_remote_file_append_reply(success: Bool) =>
    if _disposed then return end
    _D.d8s("RemoteJournalClient (last _state=%d):: " + 
      "start_remote_file_append_reply: success %s\n", _state.num(),
      success.string())
    if _state.num() == _SStartRemoteFileAppendWaiting.num() then
      if success then
        _appending = true
        _catch_up_state()
      else
        _local_size_discovery()
      end
    end

  fun ref _catch_up_state() =>
    if _disposed then return end
    _D.d866("RemoteJournalClient (last _state=%d):: " +
      "catch_up_state _local_size %d _remote_size %d\n", _state.num(),
      _local_size, _remote_size)
    _state = _SCatchUp

    if not _connected then
      _local_size_discovery()
      return
    end

    if _local_size == _remote_size then
      _D.d86("RemoteJournalClient (last _state=%d):: catch_up_state line %d\n",
        _state.num(), __loc.line())
      send_buffer_state()
    elseif _local_size > _remote_size then
      _D.d86("RemoteJournalClient (last _state=%d):: catch_up_state line %d\n",
        _state.num(), __loc.line())
      _catch_up_send_block()
    else
      _D.d86("RemoteJournalClient (last _state=%d):: catch_up_state line %d\n",
        _state.num(), __loc.line())
      Fail()
    end

  fun ref _catch_up_send_block() =>
    let missing_bytes = _local_size - _remote_size
    //let block_size = missing_bytes.min(1024*1024)
    let block_size = missing_bytes.min(50)

    _D.d6("\t_catch_up_send_block: block_size = %d\n", block_size)
    with file = File.open(_journal_fp) do
      file.seek(_remote_size.isize())
      let bytes = recover val file.read(block_size) end
      let goo = recover val [bytes] end
      _D.d66("\t_catch_up_send_block: _remote_size %d bytes size = %d\n",
        _remote_size, bytes.size())
      _dos.send_unframed(goo)
      _remote_size = _remote_size + bytes.size()
      _catch_up_state()
    end

  be send_buffer_state() =>
    _send_buffer_state()

  fun ref _send_buffer_state() =>
    if _disposed then return end
    _D.d866("RemoteJournalClient (last _state=%d):: send_buffer_state " +
      "_local_size %d _remote_size %d\n", _state.num(),
      _local_size, _remote_size)
    if _state.num() != _SCatchUp.num() then
      return
    end
    if not _appending then
      _D.d8("RemoteJournalClient (last _state=%d):: send_buffer_state " +
        "not _appending, returning\n", _state.num())
      return
    end
    if not _connected then
      _local_size_discovery()
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
      _in_sync_state()
    else
      for (offset, data, data_size) in _buffer.values() do
        if (offset + data_size) <= _remote_size then
          _D.d666("\t======send_buffer_state: " +
            "skipping offset %d data_size %d remote_size %d\n",
            offset, data_size, _remote_size)
        elseif (offset < _remote_size) and
          ((offset + data_size) > _remote_size) then
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
          _D.d666("\t======send_buffer_state: " +
            "TODO split offset %d data_size %d remote_size %d\n", offset,
            data_size, _remote_size)
          _make_new_dos_then_local_size_discovery()
          return
        elseif offset == _remote_size then
          _D.d666("\t======send_buffer_state: " +
            "send_unframed offset %d data_size %d remote_size %d\n", offset,
            data_size, _remote_size)
          // keep _local_size in sync with _remote_size
          _local_size = _local_size + data_size
          _dos.send_unframed(data)
          _remote_size = _remote_size + data_size
        else
          Fail()
        end
      end
      _buffer.clear()
      _buffer_size = 0
      _D.d866("RemoteJournalClient (last _state=%d):: " +
        "send_buffer_state _local_size %d _remote_size %d\n", _state.num(),
        _local_size, _remote_size)
      _in_sync_state()
    end

  fun ref _in_sync_state() =>
    if _disposed then return end
    _D.d866("RemoteJournalClient (last _state=%d):: " +
      "in_sync_state _local_size %d _remote_size %d\n", _state.num(),
      _local_size, _remote_size)
    if _state.num() != _SSendBuffer.num() then
      Fail()
    end
    _state = _SInSync

    _in_sync = true

  be be_writev(offset: USize, data: ByteSeqIter, data_size: USize) =>
    if _disposed then return end
    _D.d86666("RemoteJournalClient (last _state=%d):: " +
      "be_writev offset %d data_size %d, _remote_size %d _buffer_size %d\n",
      _state.num(), offset, data_size, _remote_size, _buffer_size)
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
        _D.d866666("RemoteJournalClient (last _state=%d):: " +
          "be_writev offset %d data_size %d, _remote_size %d " +
          "hey got old/delayed writev op for offset %d data_size %d\n",
          _state.num(), offset, data_size, _remote_size, offset, data_size)
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
          _D.d("\t====be_writev: be_writev CLEAR _buffer\n")
          _buffer.clear()
        end
      end
      _buffer_size = _buffer_size + data_size
      _D.d6("\t====be_writev: be_writev new _buffer_size %d\n", _buffer_size)
    end

  be dos_client_connection_status(connected: Bool) =>
    if _disposed then return end
    _D.d8s("RemoteJournalClient (last _state=%d):: " +
      "dos_client_connection_status %s\n", _state.num(), connected.string())
    _connected = connected
    if not _connected then
      _appending = false
      _in_sync = false
    end
    if _connected then
      _local_size_discovery()
    end
