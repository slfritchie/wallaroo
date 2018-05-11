use "buffered"

actor SimpleJournal2
  """
  This actor is similar to the SimpleJournal actor in the
  slf-file-io-redirector branch.  The difference is that this
  actor writes both to a local journal and to a remote journal.

  TODO adding multiple remote journal backends should be straightforward.

  TODO The slf-file-io-redirector branch should have a refactoring to
  add an interface to allow easy substitution of the journal?
  """
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
        _D.ds666s("### SimpleJournal: writev %s bytes %d pdu_size %d optag %d RET %s\n", path, bytes_size, pdu_size, optag, ret.string())
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
    _D.d6("_SJ: encode_request size %d\n", size)
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
