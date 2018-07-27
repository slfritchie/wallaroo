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

actor SimpleJournalMirror is SimpleJournal
  """
  This journal actor writes both to a local journal and to a remote journal.

  TODO adding multiple remote journal backends should be straightforward.
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
        // _D.ds666s("### SimpleJournal: writev %s bytes %d pdu_size %d optag %d RET %s\n", path, bytes_size, pdu_size, optag, ret.string())
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

  be remove(path: String, optag: USize = 0) =>
    if _j_closed then
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

////TODO fix me!
class SimpleJournalBackendRemote is SimpleJournalBackend
  ////let _rjc: RemoteJournalClient

  new create(/****rjc: RemoteJournalClient****/) =>
    None////_rjc = rjc

  fun ref be_dispose() =>
    None////_rjc.dispose()

  fun ref be_position(): USize =>
    666 // TODO

  fun ref be_writev(offset: USize, data: ByteSeqIter val, data_size: USize)
  : Bool
  =>
    // TODO offset sanity check
    // TODO offset update
    ////_D.d66("SimpleJournalBackendRemote: be_writev offset %d data_size %d\n", offset, data_size)
    ////_rjc.be_writev(offset, data, data_size)
    true

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


