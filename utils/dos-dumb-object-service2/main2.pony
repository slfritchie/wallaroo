use "buffered" // DEBUG for embedded AsyncJournal
use "collections"
use "files"
use "net"
use "promises"
use "time"

use "wallaroo_labs/mort"
use "lib/wallaroo/ent/recovery"

/*************************************************************
 Pony runtime SIGSEGV/SIGBUS reproducer instructions:

 From the top of the Wallaroo source repo, compile with:

   ponyc -d --verbose=1 --path=.:./lib ./utils/dos-dumb-object-service2

 Process 1 of 2: run the Python DOS server:

   mkdir ./yodel
   python utils/dos-dumb-object-service/dos-server.py ./yodel

 Process 2 of 2: run this reproducer:

   rm yodel/use-dir-foo/j-file-name.journal
   tar cf - /usr/lib | dd bs=1024k count=100 iflag=fullblock > ./j-file-name.journal
   ./dos-dumb-object-service2 use-dir-foo j-file-name
   ls -l ./j-file-name.journal yodel/use-dir-foo/j-file-name.journal

 Output when run on OS X Sierra:

 Bus error
 -rw-r--r--  1 scott  staff  3648280 Aug  1 17:17 ./j-file-name.journal
 -rw-rw-r--  1 scott  staff   233650 Aug  1 17:17 yodel/use-dir-foo/j-file-name.journal

 Output when run on Ubuntu Xenial 64:

 Segmentation fault (core dumped)
 -rw-r--r-- 1 vagrant vagrant 4394318 Aug  1 22:20 ./j-file-name.journal
 -rw-rw-r-- 1 vagrant vagrant       0 Aug  1 22:20 yodel/use-dir-foo/j-file-name.journal

 Factors that appear to affect the likelihood of a crash:

 - Running with `--ponythreads=1` usually makes the crash quicker,
   at least on Linux.
 - Whether compiled with `ponyc -d` or not
   - Debug/unoptimized builds with `-d` flag increases risk of crash.
   - Optimized builds without `-d` decreases risk of crash.
 - The "catch-up" write size, defined at line 413 ("let block_size") of
   lib/wallaroo/ent/recovery/remote_journal_client.pony
   - 50 or smaller greatly increases risk of crash
   - 512 or higher seems to almost never crash
 - The constant at line 393 of 
   lib/wallaroo/ent/recovery/remote_journal_client.pony
   - This constant affects max stack depth during the catchup phase by
     determining when to perform work via recursion or a behavior call.
   - A deeper stack increases risk of crash.

 *************************************************************/

/*************************************************************
OLD TEST INSTRUCTIONS:
Test instructions, run the server:

mkdir ./yodel
python2 ./utils/dos-dumb-object-service/dos-server.py ./yodel

Run the client test:

ponyc ./utils/dos-dumb-object-service
sh -c 'date ; ( for i in `seq 1 100`; do rm -fv j-file-name.journal ; rm -fv yodel/use-dir-foo/j-file-name.journal ; ./dos-dumb-object-service --ponyminthreads=8 --ponythreads=8 use-dir-foo j-file-name 2>&1 | tee foo.out ; grep "====" foo.out ; /bin/echo -n "cmp says: " ; cat /dev/null | cmp j-file-name.journal yodel/use-dir-foo/j-file-name.journal ; if [ $? -ne 0 ]; then echo STOP; exit 4; fi; done ) 2>&1 | egrep "STOP|hey" ; date'

What this does:

1. Start the server.
2. Run the test client 100x in a shell for loop.
3. Each test:
   - Removes the files that the test creates, 1 directly by the test client
     and one by the server.
   - Test client writes to a local I/O journal file + mirrored version,
     periodically driven by a Timer inside of _stage10().  The client
     exits after a certain number of timer firings.
   - Buried in dos_client.pony's DOSnotify.sent() function, there is a
     counter called _qqq_count.  When _qqq_count > _qqq_crashme, then
     the TCP connection is closed ... to simulate a TCP error without
     resorting to using Spike.

The rest of DOSclient is supposed to react to the TCP error by:

a. Reconnect to the DOS server.
b. Discover the size of the remote version of the journal file.
c. Copy any missing bytes from local -> remote journal file.
d. Resume copying new bytes from local -> remote journal file.

By the time that the client finishes running, the local & remote file
must always be identical.  I've discovered that the I/O timer
granularity differences between OS X and Ubuntu Trusty/14.04 are
significant.  I can run the Tick.apply() function about 70 times on OS
X to get stable results (executing 10K times), but Ubu requires about
350 times to finish its work reliably.

**************************************************************/

actor Main
  let _args: Array[String] val
  var _usedir_name: String = "bogus/bogus"
  var _journal_path: String = "/dev/bogus"
  var _journal: (SimpleJournal | None) = None

  new create(env: Env) =>
    _args = env.args
    try
      let auth = env.root as AmbientAuth
      let make_dos = recover val
        {(rjc: RemoteJournalClient, usedir_name: String): DOSclient =>
          DOSclient(auth, "localhost", "9999", rjc, usedir_name)
        } end
      _usedir_name = _args(1)?
      if _usedir_name == "" then
        Fail()
      end
      try
        _usedir_name.find("/")?
        Fail()
      else
        None
      end
      _journal_path = _args(2)? + ".journal"

      let journal_fp = FilePath(env.root as AmbientAuth, _journal_path)?
      let rjc = RemoteJournalClient(env.root as AmbientAuth,
        journal_fp, _journal_path, _usedir_name, make_dos)
      let j_remote = recover iso SimpleJournalBackendRemote(rjc) end

      let j_file = recover iso SimpleJournalBackendLocalFile(journal_fp) end
      _journal = SimpleJournalMirror(consume j_file, consume j_remote, false)

      stage10()
    else
      Fail()
    end

/**********************************************************/

  be stage10() =>
    try
      _stage10(_journal as SimpleJournal)
    else
      Fail()
    end

  fun _stage10(j: SimpleJournal) =>
    let ts = Timers
    let t = Timer(ScribbleSome(j, 50), 0, 2_000_000)
    ts(consume t)
    let tick = recover Tick.create(j) end
    let t2 = Timer(consume tick, 0, 5_000_000)
    ts(consume t2)
    _D.d("STAGE 10: done\n")

class ScribbleSome is TimerNotify
  let _j: SimpleJournal
  let _limit: USize
  var _c: USize = 0

  new iso create(j: SimpleJournal, limit: USize) =>
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
  let _j: SimpleJournal

  new create(j: SimpleJournal) =>
    _j = j

  fun ref apply(t: Timer, c: U64): Bool =>
    let max_count: USize = 9999 // 70 seems enough for OS X, but 350 for Linux @ 10msec jiffy??

    _D.d6("************************ Tick %d\n", _c)
    _c = _c + 1
    if _c > max_count then
      @printf[I32]("Tick count max=%d reached, stopping\n".cstring(), max_count)
      _j.dispose_journal()
      false
    else
      true
    end

