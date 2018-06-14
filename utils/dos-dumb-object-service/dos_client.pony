
use "buffered" // DEBUG for embedded AsyncJournal
use "collections"
use "files"
use "net"
use "promises"
use "time"

actor Main
  let _auth: (AmbientAuth | None)
  let _args: Array[String] val
  var _dos: (DOSclient | None) = None
  var _journal_path: String = "/dev/bogus"
  var _journal: (SimpleJournal | None) = None

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

      _dos = DOSclient(env.out, env.root as AmbientAuth, "localhost", "9999")
      let dos2 = _dos as DOSclient
      _journal_path = _args(1)? + ".journal"
      let journal_fp = FilePath(_auth as AmbientAuth, _journal_path)?
      _journal = SimpleJournal(journal_fp)

      SimpleJournalMirror(_auth as AmbientAuth, journal_fp,
        _journal as SimpleJournal, dos2)

      stage10()
    else
      Fail()
    end

/**********************************************************/

  be stage10() =>
    try
      _stage10(_journal as SimpleJournal, _dos as DOSclient)
    else
      Fail()
    end

  fun _stage10(j: SimpleJournal, dos_c: DOSclient) =>
    @usleep[None](U32(100_1000))
    let ts = Timers
    let t = Timer(ScribbleSome(j), 0, 50_000_000)
    ts(consume t)
    @printf[I32]("STAGE 10: done\n".cstring())

class ScribbleSome is TimerNotify
  let _j: SimpleJournal
  var _c: USize = 0

  new iso create(j: SimpleJournal) =>
    _j = j

  fun ref apply(t: Timer, c: U64): Bool =>
    let abc = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

    if _c > 10 then
      @printf[I32]("TIMER: counter expired, stopping\n".cstring())
      false
    else
      @printf[I32]("TIMER: counter %d\n".cstring(), c)
      let goo = recover val [abc] end
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

/**********************************************************/

actor SimpleJournalMirror
  // TODO not sure which vars we really need
  let _auth: AmbientAuth
  let _journal_fp: FilePath
  let _journal: SimpleJournal
  let _dos: DOSclient
  var _local_size: USize = 0
  var _remote_size: USize = 0

  new create(auth: AmbientAuth, journal_fp: FilePath,
    journal: SimpleJournal, dos: DOSclient
  ) =>
    _auth = auth
    _journal_fp = journal_fp
    _journal = journal
    _dos = dos
    @printf[I32]("AsyncJournalMirror: create\n".cstring())
    local_size_discovery()

  be local_size_discovery() =>
    @printf[I32]("AsyncJournalMirror: local_size_discovery for %s\n".cstring(),
      _journal_fp.path.cstring())
    try
      let info = FileInfo(_journal_fp)?
      @printf[I32]("AsyncJournalMirror: %s size %d\n".cstring(), _journal_fp.path.cstring(), info.size)
      _local_size = info.size
      remote_size_discovery(1_000_000, 1_000_000_000)
    else
      // We expect that this file will exist very very shortly.  Spinwait.
      local_size_discovery()
    end

  be remote_size_discovery(sleep_time: USize, max_time: USize) =>
    @printf[I32]("AsyncJournalMirror: remote_size_discovery for %s\n".cstring(),
      _journal_fp.path.cstring())
    let rsd = recover tag this end
    let p = Promise[DOSreply]
    p.next[None](
      {(a)(rsd) =>
        @printf[I32]("PROMISE: I got array of size %d\n".cstring(), a.size())
        var remote_size: USize = 0
        try
          for (file, size, appending) in (a as DOSreplyLS).values() do
            @printf[I32]("\t %s,%d,%s\n".cstring(),
              file.cstring(), size, appending.string().cstring())
            if file == _journal_fp.path then
              @printf[I32]("\tFound it")
              remote_size = size
              break
            end
          end
          rsd.start_remote_file_append(remote_size)
        end
      },
      {()(rsd, sleep_time, max_time) =>
        @printf[I32]("PROMISE: remote_size_discovery BUMMER!\n".cstring())

        let ts = Timers
        let later = DoLater(recover
          {(): Bool =>
            rsd.remote_size_discovery(sleep_time*2, max_time)
            false
          } end)
        let t = Timer(consume later, U64.from[USize](sleep_time.min(max_time)), 0)
        ts(consume t)
      })
    _dos.do_ls(p)

  be start_remote_file_append(remote_size: USize) =>
    _remote_size = remote_size
    @printf[I32]("AsyncJournalMirror: start_remote_file_append for %s\n".cstring(), _journal_fp.path.cstring())
    @printf[I32]("AsyncJournalMirror: start_remote_file_append _local_size %d _remote_size %d\n".cstring(), _local_size, _remote_size)

/**********************************************************/

type DOSreplyLS is Array[(String, USize, Bool)] val
type DOSreply is (String val| DOSreplyLS val)
primitive DOSgetchunk
primitive DOSls
primitive DOSnoop
type DOSop is (DOSgetchunk | DOSls | DOSnoop)

actor DOSclient
  let _out: OutStream
  var _auth: (AmbientAuth | None) = None
  let _host: String
  let _port: String
  var _sock: (TCPConnection | None) = None
  var _connected: Bool = false
  var _do_reconnect: Bool = true
  let _waiting_reply: Array[(DOSop, (Promise[DOSreply]| None))] = _waiting_reply.create()

  new create(out: OutStream, auth: AmbientAuth, host: String, port: String) =>
    _out = out
    _auth = auth
    _host = host
    _port = port
    _reconn()

  fun ref _reconn (): None =>
    ifdef "verbose" then
      _out.print("DOS: calling _reconn")
    end
    try
      _sock = TCPConnection(_auth as AmbientAuth,
        recover DOSnotify(this, _out) end, _host, _port)
    end

  be dispose() =>
    ifdef "verbose" then
      _out.print("DOS: &&&&&dispose")
    end
    _do_reconnect = false
    _dispose()

  fun ref _dispose() =>
    ifdef "verbose" then
      _out.print("DOS: _dispose.  Promises to reject: " + _waiting_reply.size().string())
    end
    try (_sock as TCPConnection).dispose() end
    _connected = false
    for (op, p) in _waiting_reply.values() do
      match p
      | None => None
      | let pp: Promise[DOSreply] =>
        pp.reject()
      end
    end
    _waiting_reply.clear()
    _connected = false

  be connected() =>
@printf[I32]("DOS: connected\n".cstring())
    ifdef "verbose" then
      _out.print("DOS: connected")
    end
    _connected = true

  be disconnected() =>
@printf[I32]("DOS: disconnected\n".cstring())
    ifdef "verbose" then
      _out.print("DOS: disconnected")
    end
    _dispose()
    if _do_reconnect then
      _reconn()
    end

  be do_ls(p: (Promise[DOSreply] | None) = None) =>
    let request: String iso = recover String end

    if _connected then
      ifdef "verbose" then
        _out.print("DOSc: do_ls")
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
          _out.print("DOSclient: ERROR: ls not connected, no promise!")
        end
        None
      | let pp: Promise[DOSreply] =>
        ifdef "verbose" then
          _out.print("DOSclient: ERROR: ls not connected, reject!  TODO")
        end
        pp.reject()
      end
    end

  be do_get_chunk(filename: String, offset: USize, size: USize,
    p: (Promise[DOSreply] | None) = None)
  =>
    let request: String iso = recover String end

    if _connected then
      let pdu: String = "g" + filename + "\t" + offset.string() + "\t" + size.string()
      ifdef "verbose" then
        _out.print("DOSc: do_get_chunk: " + pdu)
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
          _out.print("DOSclient: ERROR: get_chunk not connected, no promise!  TODO")
        end
        None
      | let pp: Promise[DOSreply] =>
        ifdef "verbose" then
          _out.print("DOSclient: ERROR: get_chunk not connected, reject!  TODO")
        end
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
          _out.print("PROMISE BIG: yay")
        end
        notify_get_file_complete(true, ts)
        },
      {(): None =>
        _out.print("PROMISE BIG: BOOOOOO")
        let empty_array: Array[T] val = recover empty_array.create() end
       notify_get_file_complete(false, empty_array)
      })

  // Used only by the DOSnotify socket thingie
  be response(data: Array[U8] iso) =>
    let str = String.from_array(consume data)
    ifdef "verbose" then
      _out.print("DOSclient GOT:" + str)
    end
    try
      (let op, let p) = _waiting_reply.shift()?
      match p
      | None =>
        None
      | let pp: Promise[DOSreply] =>
        try
          match op
          | DOSls =>
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
  var _qqq_crashme: USize = 6

  new create(client: DOSclient, out: OutStream) =>
    _client = client
    _out = out

  fun ref connect_failed(conn: TCPConnection ref) =>
    ifdef "verbose" then
      _out.print("SOCK: connect_failed")
    end

  fun ref connected(conn: TCPConnection ref) =>
    ifdef "verbose" then
      _out.print("SOCK: I am connected.")
    end
    _header = true
    conn.expect(4)
    _client.connected()

  fun ref received(
    conn: TCPConnection ref,
    data: Array[U8] iso,
    times: USize)
    : Bool
  =>
    if _header then
      ifdef "verbose" then
        _out.print("SOCK: received header")
      end
      try
        let expect = Bytes.to_u32(data(0)?, data(1)?, data(2)?, data(3)?).usize()
        conn.expect(expect)
        _header = false
      else
        ifdef "verbose" then
          _out.print("Error reading header on control channel")
        end
      end
    else
      ifdef "verbose" then
        _out.print("SOCK: received payload")
      end
      _client.response(consume data)
      conn.expect(4)
      _header = true
    end
    false

  fun ref sent(conn: TCPConnection ref, data: ByteSeq): ByteSeq =>
    ifdef "verbose" then
      _out.print("SOCK: sent")
    end
    _qqq_crashme = _qqq_crashme - 1
    if _qqq_crashme == 0 then
      conn.close()
    end
    data

  fun ref sentv(conn: TCPConnection ref, data: ByteSeqIter): ByteSeqIter =>
    ifdef "verbose" then
      _out.print("SOCK: sentv")
    end
    data

  fun ref closed(conn: TCPConnection ref) =>
    ifdef "verbose" then
      _out.print("SOCK: closed")
    end
    _client.disconnected()

primitive Bytes
  fun to_u32(a: U8, b: U8, c: U8, d: U8): U32 =>
    (a.u32() << 24) or (b.u32() << 16) or (c.u32() << 8) or d.u32()

/****************************/
/* Copied from backend.pony */
/****************************/

class AsyncJournalledFile
  let _file_path: String
  let _file: File
  let _journal: SimpleJournal
  let _auth: AmbientAuth
  var _offset: USize
  let _do_local_file_io: Bool
  var _tag: USize = 1

  new create(filepath: FilePath, journal: SimpleJournal,
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
  be async_io_ok(j: SimpleJournal tag, optag: USize)
  be async_io_error(j: SimpleJournal tag, optag: USize)

actor SimpleJournal
  var filepath: FilePath
  var _j_file: File
  var _j_file_closed: Bool
  let _encode_io_ops: Bool
  let _owner: (None tag | SimpleJournalAsyncResponseReceiver tag)

  new create(filepath': FilePath, encode_io_ops: Bool = true,
    owner: (None tag | SimpleJournalAsyncResponseReceiver tag) = None)
  =>
    filepath = filepath'
    _encode_io_ops = encode_io_ops
    _owner = owner

    _j_file = File(filepath)
    _j_file_closed = false
    // A newly created file has offset @ start of file, we want the end of file
    _j_file.seek_end(0)

  // TODO This method only exists because of prototype hack laziness
  // that does not refactor both RotatingEventLog & SimpleJournal.
  // It is used only by RotatingEventLog.
  be dispose_journal() =>
    _j_file.dispose()
    _j_file_closed = true

  be set_length(path: String, len: USize, optag: USize = 0) =>
    if _j_file_closed then
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
      _j_file.writev(wb.done())
    else
      Fail()
    end

  be writev(path: String, data: ByteSeqIter val, optag: USize = 0) =>
    if _j_file_closed then
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
        _j_file.writev(wb.done())
      else
        _j_file.writev(data)
      end
    if not write_res then
      // We don't know how many bytes were written.  ^_^
      // TODO So, I suppose we need to ask the OS about the file size
      // to figure that out so that we can do <TBD> to recover.
      try
        let o = _owner as (SimpleJournalAsyncResponseReceiver tag)
        o.async_io_error(this, optag)
      end
    end
    if optag > 0 then
      try
        let o = _owner as (SimpleJournalAsyncResponseReceiver tag)
        o.async_io_ok(this, optag)
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
    let size = wb.size()
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
