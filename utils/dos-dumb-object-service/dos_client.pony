
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
      {() => env.out.print("PROMISE: BUMMER!")}
    )
    dos.do_ls(p0)

    // got_a_chunk is {(DOSreply): None} val
    let got_a_chunk = {(chunk: DOSreply): None =>
        @printf[I32]("PROMISE: 0x%x: I got a chunk of size %d\n".cstring(),
          this, chunk.size())
        try @printf[I32](">>>%s<<<\n".cstring(), (chunk as String).cstring()) end
      }
    let failed_a_chunk = {(): None =>
         @printf[I32]("PROMISE: 0x%x: BUMMER!\n".cstring(),
          this)
      }

    let p0b = Promise[DOSreply]
    // Fulfill needs an iso, so we can't use got_a_chunk directly as an arg.
    p0b.next[None](
      {(chunk: DOSreply): None => got_a_chunk.apply(chunk) },
      {() => failed_a_chunk.apply() })
    dos.do_get_chunk("bar", 0, 0, p0b)

    let file_notify = {(success: Bool, num_chunks: USize) =>
      @printf[I32]("PROMISE: 0x%x: entire file transfer success for %s num_chunks %d\n".cstring(),
        this, success.string().cstring(), num_chunks)
      }
    dos.get_file("bar", 47, 10, got_a_chunk, failed_a_chunk, file_notify)
    // dos.get_file("bar", 47, 10, got_a_chunk, failed_a_chunk, file_notify)

type DOSreplyLS is Array[(String, USize, Bool)] val
type DOSreply is (String val| DOSreplyLS val)
primitive DOSgetchunk
primitive DOSls
primitive DOSnoop
type DOSop is (DOSgetchunk | DOSls | DOSnoop)

actor DOSclient
  var _sock: (TCPConnection | None) = None
  let _out: OutStream
  var _connected: Bool = false
  let _waiting_reply: Array[(DOSop, (Promise[DOSreply]| None))] = _waiting_reply.create()

  new create(env: Env, host: String, port: String) =>
    _out = env.out
    try
      _sock = TCPConnection(env.root as AmbientAuth,
        recover DOSnotify(this, env.out) end, "localhost", "9999")
    end

  be connected() =>
    _connected = true

  be disconnected() =>
    _connected = false
    for (op, p) in _waiting_reply.values() do
      match p
      | None => None
      | let pp: Promise[DOSreply] =>
        pp.reject()
      end
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
      _out.print("DOSclient: ERROR: not connected!  TODO")
      match p
      | None =>
        None
      | let pp: Promise[DOSreply] =>
        pp.reject()
      end
    end

  be do_get_chunk(filename: String, offset: USize, size: USize,
    p: (Promise[DOSreply] | None) = None)
  =>
    let request: String iso = recover String end

    if _connected then
      let pdu: String = "g" + filename + "\t" + offset.string() + "\t" + size.string()
      _out.print("DOSc: do_get_chunk: " + pdu)

      request.push(0)
      request.push(0)
      request.push(0)
      request.push(pdu.size().u8()) // TODO: bogus if request size > 255
      request.append(pdu)
      try (_sock as TCPConnection).write(consume request) end
      _waiting_reply.push((DOSgetchunk, p))
    else
      _out.print("DOSclient: ERROR: not connected!  TODO")
      match p
      | None =>
        None
      | let pp: Promise[DOSreply] =>
        pp.reject()
      end
    end

  be get_file(filename: String, file_size: USize, chunk_size: USize,
    chunk_success: {(DOSreply): None} val, chunk_failed: {(): None} val,
    file_notify: {(Bool, USize): None} val)
  =>
    let chunk_ps: Array[Promise[Bool]] = chunk_ps.create()

    for i in Range[USize](0, file_size, chunk_size) do
      let p0 = Promise[Bool]
      // p0.next[None]({(x: Bool) => None}, {() => None})
      p0.next[None](
        {(x: Bool) =>
          @printf[I32]("PROMISE: 0x%lx: Yay, p0 chunk at offset %d\n".cstring(), this, i)
          None
        },
        {() =>
          @printf[I32]("PROMISE: 0x%lx: BOO, p0 chunk at offset %d\n".cstring(), this, i)
          None
        })
      chunk_ps.push(p0)

      let p1 = Promise[DOSreply]
      // Fulfill needs an iso, so we can't use got_a_chunk directly as an arg.
      p1.next[None](
        {(chunk: DOSreply): None =>
          @printf[I32]("PROMISE: 0x%lx: Yay, p1 chunk at offset %d\n".cstring(), this, i)
          chunk_success.apply(chunk)
          p0.apply(true)
        },
        {() =>
          @printf[I32]("PROMISE: 0x%lx: BOO, p1 chunk at offset %d\n".cstring(), this, i)
          chunk_failed.apply()
          p0.reject()
        })
      do_get_chunk(filename, i, chunk_size, p1)
    end

    let p_all_chunks1 = Promises[Bool].join(chunk_ps.values())
    p_all_chunks1.next[None](
      {(bools: Array[Bool] val): None =>
        @printf[I32]("PROMISE BIG: 0x%lx: yay\n".cstring(), this)
        file_notify(true, bools.size())
        },
      {(): None =>
        @printf[I32]("PROMISE BIG: *******************\n\n\n".cstring())
        @printf[I32]("PROMISE BIG: 0x%lx: BOOOOOO\n".cstring(), this)
       file_notify(false, 0)
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
      end
    else
      _out.print("DOSclient: response: should never happen")
    end

class DOSnotify is TCPConnectionNotify
  let _client: DOSclient
  let _out: OutStream
  var _header: Bool = true
  var _qqq_crashme: USize = 7

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
