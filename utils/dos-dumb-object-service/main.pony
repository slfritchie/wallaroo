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
    let ts = Timers
    let t = Timer(ScribbleSome(j, 50), 0, 2_000_000)
    ts(consume t)
    let tick = recover Tick.create(j) end
    let t2 = Timer(consume tick, 0, 5_000_000)
    ts(consume t2)
    _D.d("STAGE 10: done\n")

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
      _D.d6("TIMER: counter limit at %d, stopping\n", _limit)
      false
    else
      _D.d6("TIMER: counter %d\n", _c)
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
    _D.d6("************************ Tick %d\n", _c)
    _c = _c + 1
    if _c > 70 then // 70 seems enough for OS X, but 234 for Linux weird TODO?
      _D.d("I am going to sleep for a few seconds before dispose()\n")
      @sleep[None](U32(10))
      _j.dispose_journal()
      false
    else
      true
    end

