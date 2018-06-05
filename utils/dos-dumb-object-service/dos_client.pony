
use "files"
use "net"
use "promises"

actor Main
  new create(env: Env) =>
    let dos = DOSclient(env, "localhost", "9999")
    let p = Promise[DOSreply]
    p.next[None](
      {(a) => env.out.print("PROMISE: I got array of size " + a.size().string())},
      {() => env.out.print("PROMISE: BUMMER!")}
    )

    @usleep[None](U32(100_000))
    dos.send_ls(p)

type DOSreply is (String val| Array[(String, USize, Bool)] val)

actor DOSclient
  var _sock: (TCPConnection | None) = None
  let _out: OutStream
  var _connected: Bool = false
  let _waiting_reply: Array[(Promise[DOSreply]| None)] = _waiting_reply.create()

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
    for p in _waiting_reply.values() do
      match p
      | None => None
      | let pp: Promise[DOSreply] =>
        pp.reject()
      end
    end

  be send_ls(p: (Promise[DOSreply] | None) = None) =>
    let request: String iso = recover String end

    if _connected then
      _out.print("DOSc: send_ls")
      request.push(0)
      request.push(0)
      request.push(0)
      request.push(1)
      request.append("l")
      try (_sock as TCPConnection).write(consume request) end
      _waiting_reply.push(p)
    else
      _out.print("DOSclient: ERROR: not connected!  TODO")
      match p
      | None =>
        None
      | let pp: Promise[DOSreply] =>
        pp.reject()
      end
    end

  be response(data: Array[U8] iso) =>
    let str = String.from_array(consume data)
    _out.print("DOSclient GOT:" + str)
    try
      let p = _waiting_reply.shift()?
      match p
      | None => None
      | let pp: Promise[DOSreply] =>
        let lines = recover val str.split("\n") end
        let res: Array[(String, USize, Bool)] iso = recover res.create() end

        for l in lines.values() do
          let fs = l.split("\t")
          if fs.size() == 0 then
            break
          end
          let file = fs(0)?
          let size = fs(1)?.usize()?
          let b = if fs(2)? == "no" then false else true end
          res.push((file, size, b))
        end
        pp(consume res)
      end
    else
      _out.print("DOSclient: response: should never happen")
    end

class DOSnotify is TCPConnectionNotify
  let _client: DOSclient
  let _out: OutStream
  var _header: Bool = true

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
    data

  fun ref sentv(conn: TCPConnection ref, data: ByteSeqIter): ByteSeqIter =>
    _out.print("SOCK: sentv")
    data

  fun ref closed(conn: TCPConnection ref) =>
    _client.disconnected()

primitive Bytes
  fun to_u32(a: U8, b: U8, c: U8, d: U8): U32 =>
    (a.u32() << 24) or (b.u32() << 16) or (c.u32() << 8) or d.u32()
