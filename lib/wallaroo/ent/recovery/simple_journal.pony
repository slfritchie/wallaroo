/*

Copyright 2018 The Wallaroo Authors.

Licensed as a Wallaroo Enterprise file under the Wallaroo Community
License (the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

     https://github.com/wallaroolabs/wallaroo/blob/master/LICENSE

*/

use "buffered"
use "files"
use "wallaroo_labs/mort"

trait SimpleJournalAsyncResponseReceiver
  be async_io_ok(j: SimpleJournal tag, optag: USize)
  be async_io_error(j: SimpleJournal tag, optag: USize)

trait tag SimpleJournal
  be dispose_journal()
  be set_length(path: String, len: USize, optag: USize = 0)
  be writev(path: String, data: ByteSeqIter val, optag: USize = 0)
  be remove(path: String, optag: USize = 0)

actor SimpleJournalNoop is SimpleJournal
  new create() =>
    None
  be dispose_journal() =>
    None
  be set_length(path: String, len: USize, optag: USize = 0) =>
    None
  be writev(path: String, data: ByteSeqIter val, optag: USize = 0) =>
    None
  be remove(path: String, optag: USize = 0) =>
    None

actor SimpleJournalLocalFile is SimpleJournal
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

  be remove(path: String, optag: USize = 0) =>
    if _j_file_closed then
      Fail()
    end
    if _encode_io_ops then
      (let pdu, let pdu_size) = _SJ.encode_request(optag, _SJ.remove(),
        recover [] end, recover [path] end)
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
  fun remove(): U8 => 2

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



