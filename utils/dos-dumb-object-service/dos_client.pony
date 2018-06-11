
use "collections"
use "files"
use "net"
use "promises"

actor Main
  new create(env: Env) =>
    let dos = DOSclient(env, "localhost", "9999")
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
        try @printf[I32](">>>%s<<<\n".cstring(), (chunk as String).cstring()) end
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
      @printf[I32]("PROMISE: 0x%x: entire file transfer for %s was success %s num_chunks %d\n".cstring(),
        this, path1.cstring(), success.string().cstring(), bs.size())
      for i in bs.values() do
        @printf[I32]("%s,".cstring(), i.string().cstring())
      end
      @printf[I32]("\n".cstring())
      }
    end
    dos.do_get_file[Bool](path1, 47, 10, got_a_chunk, failed_a_chunk, notify_get_file_complete)

    @printf[I32]("BEFORE SLEEP\n".cstring())
    @usleep[None](U32(300_000))
    @printf[I32]("AFTER SLEEP\n".cstring())
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
    @usleep[None](U32(300_000))
    dos.dispose()

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

  new create(env: Env, host: String, port: String) =>
    _out = env.out
    try _auth = env.root as AmbientAuth end
    _host = host
    _port = port
    _reconn()

  fun ref _reconn (): None =>
    @printf[I32]("DOS: calling _reconn\n".cstring())
    try
      _sock = TCPConnection(_auth as AmbientAuth,
        recover DOSnotify(this, _out) end, _host, _port)
    end

  be dispose() =>
    @printf[I32]("DOS: &&&&&dispose\n".cstring())
    _do_reconnect = false
    _dispose()

  fun ref _dispose() =>
    @printf[I32]("DOS: _dispose.  NOTE: %d promises to reject\n".cstring(), _waiting_reply.size())
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
    _connected = true

  be disconnected() =>
    @printf[I32]("DOS: disconnected\n".cstring())
    _dispose()
    if _do_reconnect then
      _reconn()
    end

  be do_ls(p: (Promise[DOSreply] | None) = None) =>
    let request: String iso = recover String end

    if _connected then
      _out.print("DOSc: do_ls")
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
        // _out.print("DOSclient: ERROR: ls not connected, no promise!  TODO")
        None
      | let pp: Promise[DOSreply] =>
        // _out.print("DOSclient: ERROR: ls not connected, reject!  TODO")
        pp.reject()
      end
    end

  be do_get_chunk(filename: String, offset: USize, size: USize,
    p: (Promise[DOSreply] | None) = None)
  =>
    let request: String iso = recover String end

    if _connected then
      let pdu: String = "g" + filename + "\t" + offset.string() + "\t" + size.string()
      // _out.print("DOSc: do_get_chunk: " + pdu)

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
        // _out.print("DOSclient: ERROR: get_chunk not connected, no promise!  TODO")
        None
      | let pp: Promise[DOSreply] =>
        // _out.print("DOSclient: ERROR: get_chunk not connected, reject!  TODO")
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
        @printf[I32]("PROMISE BIG: 0x%lx: yay\n".cstring(), this)
        notify_get_file_complete(true, ts)
        },
      {(): None =>
        @printf[I32]("PROMISE BIG: 0x%lx: BOOOOOO\n".cstring(), this)
        let empty_array: Array[T] val = recover empty_array.create() end
       notify_get_file_complete(false, empty_array)
      })

  // Used only by the DOSnotify socket thingie
  be response(data: Array[U8] iso) =>
    let str = String.from_array(consume data)
    // _out.print("DOSclient GOT:" + str)
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
    _out.print("SOCK: connect_failed")

  fun ref connected(conn: TCPConnection ref) =>
    _out.print("SOCK: I am connected.")
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
      _out.print("SOCK: received header")
      try
        let expect = Bytes.to_u32(data(0)?, data(1)?, data(2)?, data(3)?).usize()
        // @printf[I32]("DBG: expecting %d bytes\n".cstring(), expect)
        conn.expect(expect)
        _header = false
      else
        @printf[I32]("Error reading header on control channel\n".cstring())
      end
    else
      _out.print("SOCK: received payload")
      _client.response(consume data)
      conn.expect(4)
      _header = true
    end
    false

  fun ref sent(conn: TCPConnection ref, data: ByteSeq): ByteSeq =>
    _out.print("SOCK: sent")
    _qqq_crashme = _qqq_crashme - 1
    if _qqq_crashme == 0 then
      conn.close()
    end
    data

  fun ref sentv(conn: TCPConnection ref, data: ByteSeqIter): ByteSeqIter =>
    _out.print("SOCK: sentv")
    data

  fun ref closed(conn: TCPConnection ref) =>
    _out.print("SOCK: closed")
    _client.disconnected()

primitive Bytes
  fun to_u32(a: U8, b: U8, c: U8, d: U8): U32 =>
    (a.u32() << 24) or (b.u32() << 16) or (c.u32() << 8) or d.u32()
