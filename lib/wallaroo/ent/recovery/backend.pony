/*

Copyright 2017 The Wallaroo Authors.

Licensed as a Wallaroo Enterprise file under the Wallaroo Community
License (the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

     https://github.com/wallaroolabs/wallaroo/blob/master/LICENSE

*/

use "buffered"
use "collections"
use "files"
use "format"
use "wallaroo_labs/conversions"
use "wallaroo/core/common"
use "wallaroo_labs/mort"
use "wallaroo/core/invariant"
use "wallaroo/core/messages"

// (is_watermark, resilient_id, uid, frac_ids, statechange_id, seq_id, payload)
type LogEntry is (Bool, StepId, U128, FractionalMessageId, U64, U64,
  Array[ByteSeq] val)

// used to hold a receovered log entry that might need to be replayed on
// recovery
// (resilient_id, uid, frac_ids, statechange_id, seq_id, payload)
type ReplayEntry is (StepId, U128, FractionalMessageId, U64, U64, ByteSeq val)

//////////////////////////////////
// Helpers for RotatingFileBackend
//////////////////////////////////
primitive HexOffset
  fun apply(offset: U64): String iso^ =>
    Format.int[U64](where x=offset, fmt=FormatHexBare, width=16,
      fill=48)

  fun u64(hex: String): U64 ? =>
    hex.u64(16)?

primitive FilterLogFiles
  fun apply(base_name: String, suffix: String = ".evlog",
    entries: Array[String] iso): Array[String]
  =>
    let es: Array[String] val = consume entries
    recover
      let filtered: Array[String] ref = Array[String]
      for e in es.values() do
        try
          if (e.find(base_name)? == 0) and
             (e.rfind(suffix)? == (e.size() - suffix.size()).isize()) then
            filtered.push(e)
          end
        end
      end
      Sort[Array[String], String](filtered)
      filtered
    end

primitive LastLogFilePath
  fun apply(base_name: String, suffix: String = ".evlog", base_dir: FilePath):
    FilePath ?
  =>
    let dir = Directory(base_dir)?
    let filtered = FilterLogFiles(base_name, suffix, dir.entries()?)
    let last_string = filtered(filtered.size()-1)?
    FilePath(base_dir, last_string)?

/////////////////////////////////
// BACKENDS
/////////////////////////////////

trait Backend
  fun ref sync() ?
  fun ref datasync() ?
  fun ref start_log_replay()
  fun ref write(): USize ?
  fun ref encode_entry(entry: LogEntry)
  fun bytes_written(): USize

class DummyBackend is Backend
  let _event_log: EventLog
  new create(event_log: EventLog) =>
    _event_log = event_log
  fun ref sync() => None
  fun ref datasync() => None
  fun ref start_log_replay() =>
    _event_log.log_replay_finished()
  fun ref write(): USize => 0
  fun ref encode_entry(entry: LogEntry) => None
  fun bytes_written(): USize => 0

class FileBackend is Backend
  //a record looks like this:
  // - is_watermark boolean
  // - producer id
  // - seq id (low watermark record ends here)
  // - uid
  // - size of fractional id list
  // - fractional id list (may be empty)
  // - statechange id
  // - payload

  let _file: AsyncJournalledFile iso
  let _file_path: String
  let _event_log: EventLog
  let _the_journal: SimpleJournal
  let _writer: Writer iso
  var _replay_log_exists: Bool
  var _bytes_written: USize = 0
  var _do_local_file_io: Bool = true

  new create(filepath: FilePath, event_log: EventLog,
    the_journal: SimpleJournal, auth: AmbientAuth,
    do_local_file_io: Bool)
  =>
    _writer = recover iso Writer end
    _file_path = filepath.path
    _replay_log_exists = filepath.exists()
    _file = recover iso
      AsyncJournalledFile(filepath, the_journal, auth, do_local_file_io) end
    _event_log = event_log
    _the_journal = the_journal
    _do_local_file_io = do_local_file_io

  fun ref dispose() =>
    _file.dispose()
    // This journal prototype uses a single SimpleJournal per EventLog
    // log file ... which is a hack to avoid re-writing a large amount
    // of EventLog.  The append-only log file rotation ought to be
    // something that SimpleJournal takes full responsibility for.
    _the_journal.dispose_journal()

  fun bytes_written(): USize =>
    _bytes_written

  fun get_path(): String =>
    _file_path

  fun ref start_log_replay() =>
    if _replay_log_exists then
      @printf[I32](("RESILIENCE: Replaying from recovery log file: " +
        _file_path + "\n").cstring())

      //replay log to EventLog
      try
        let r = Reader

        //seek beginning of file
        _file.seek_start(0)
        var size = _file.size()
        _bytes_written = size

        var num_replayed: USize = 0
        var num_skipped: USize = 0

        // array to hold recovered data temporarily until we've sent it off to
        // be replayed
        var replay_buffer: Array[ReplayEntry val] ref = replay_buffer.create()

        let watermarks_trn = recover trn Map[StepId, SeqId] end

        //start iterating until we reach original EOF
        while _file.position() < size do
          r.append(_file.read(25))
          let is_watermark = BoolConverter.u8_to_bool(r.u8()?)
          let resilient_id = r.u128_be()?
          let seq_id = r.u64_be()?
          if is_watermark then
            ifdef debug then
              Invariant(
                try
                  let last_seq_id = watermarks_trn(resilient_id)?
                  seq_id > last_seq_id
                else
                  true
                end
              )
            end

            // save last watermark read from file
            watermarks_trn(resilient_id) = seq_id
          else
            r.append(_file.read(24))
            let uid = r.u128_be()?
            let fractional_size = r.u64_be()?
            let frac_ids = recover val
              if fractional_size > 0 then
                let bytes_to_read = fractional_size.usize() * 4
                r.append(_file.read(bytes_to_read))
                let l = Array[U32]
                for i in Range(0,fractional_size.usize()) do
                  l.push(r.u32_be()?)
                end
                l
              else
                //None is faster if we have no frac_ids, which will probably be
                //true most of the time
                None
              end
            end
            r.append(_file.read(16))
            let statechange_id = r.u64_be()?
            let payload_length = r.u64_be()?
            let payload = recover val
              if payload_length > 0 then
                _file.read(payload_length.usize())
              else
                Array[U8]
              end
            end

            // put entry into temporary recovered buffer
            replay_buffer.push((resilient_id, uid, frac_ids, statechange_id,
              seq_id, payload))
          end

          // clear read buffer to free file data read so far
          if r.size() > 0 then
            Fail()
          end
          r.clear()
        end

        let watermarks = consume val watermarks_trn

        _event_log.initialize_seq_ids(watermarks)

        // iterate through recovered buffer and replay entries at or below
        // watermark
        for entry in replay_buffer.values() do
          // only replay if at or below watermark
          if entry._5 <= watermarks.get_or_else(entry._1, 0) then
            num_replayed = num_replayed + 1
            _event_log.replay_log_entry(entry._1, entry._2, entry._3,
              entry._4, entry._6)
          else
            num_skipped = num_skipped + 1
          end
        end

        @printf[I32](("RESILIENCE: Replayed %d entries from recovery log " +
          "file.\n").cstring(), num_replayed)
        @printf[I32](("RESILIENCE: Skipped %d entries from recovery log " +
          "file.\n").cstring(), num_skipped)

        _file.seek_end(0)
        _event_log.log_replay_finished()
      else
        @printf[I32]("Cannot recover state from eventlog\n".cstring())
      end
    else
      @printf[I32]("RESILIENCE: Could not find log file to replay.\n"
        .cstring())
      Fail()
    end

  fun ref write(): USize ?
  =>
    let size = _writer.size()
    if not _file.writev(recover val _writer.done() end) then
      error
    else
      _bytes_written = _bytes_written + size
    end
    _bytes_written

  fun ref encode_entry(entry: LogEntry)
  =>
    (let is_watermark: Bool, let resilient_id: StepId,
     let uid: U128, let frac_ids: FractionalMessageId,
     let statechange_id: U64, let seq_id: U64,
     let payload: Array[ByteSeq] val) = entry

    ifdef "trace" then
      if is_watermark then
        @printf[I32]("Writing Watermark: %d\n".cstring(), seq_id)
      else
        @printf[I32]("Writing Message: %d\n".cstring(), seq_id)
      end
    end

    _writer.u8(BoolConverter.bool_to_u8(is_watermark))
    _writer.u128_be(resilient_id)
    _writer.u64_be(seq_id)

    if not is_watermark then
      _writer.u128_be(uid)

      match frac_ids
      | None =>
        _writer.u64_be(0)
      | let x: Array[U32] val =>
        let fractional_size = x.size().u64()
        _writer.u64_be(fractional_size)

        for frac_id in x.values() do
          _writer.u32_be(frac_id)
        end
      end

      _writer.u64_be(statechange_id)
      var payload_size: USize = 0
      for p in payload.values() do
        payload_size = payload_size + p.size()
      end
      _writer.u64_be(payload_size.u64())

      // write data to write buffer
      _writer.writev(payload)
    end

  fun ref sync() ? =>
    _file.sync()
    match _file.errno()
    | FileOK => None
    else
@printf[I32]("DBG sync line %d\n".cstring(), __loc.line())
      error
    end

  fun ref datasync() ? =>
    _file.datasync()
    match _file.errno()
    | FileOK => None
    else
      error
    end

class RotatingFileBackend is Backend
  // _basepath identifies the worker
  // For unique file identifier, we use the sum of payload sizes saved as a
  // U64 encoded in hex. This is maintained with _offset and
  // _backend.bytes_written()
  var _backend: FileBackend
  let _base_dir: FilePath
  let _base_name: String
  let _suffix: String
  let _event_log: EventLog
  let _the_journal: SimpleJournal
  let _auth: AmbientAuth
  let _do_local_file_io: Bool
  let _file_length: (USize | None)
  var _offset: U64
  var _rotate_requested: Bool = false

  new create(base_dir: FilePath, base_name: String, suffix: String = ".evlog",
    event_log: EventLog, file_length: (USize | None),
    the_journal: SimpleJournal, auth: AmbientAuth, do_local_file_io: Bool) ?
  =>
    _base_dir = base_dir
    _base_name = base_name
    _suffix = suffix
    _file_length = file_length
    _event_log = event_log
    _the_journal = the_journal
    _auth = auth
    _do_local_file_io = do_local_file_io

    // scan existing files matching _base_path, and identify the latest one
    // based on the hex offset
    _offset = try
      let last_file_path = LastLogFilePath(_base_name, _suffix, _base_dir)?
      let parts = last_file_path.path.split("-.")
      let offset_str = parts(parts.size()-2)?
      HexOffset.u64(offset_str)?
    else // create a new file with offset 0
      0
    end
    let p = _base_name + "-" + HexOffset(_offset) + _suffix
    let fp = FilePath(_base_dir, p)?
    let local_journal_filepath = FilePath(_base_dir, p + ".bin")?
    let local_journal = SimpleJournal(local_journal_filepath, false, _event_log)
    _backend = FileBackend(fp, _event_log, local_journal, _auth, _do_local_file_io)

  fun bytes_written(): USize => _backend.bytes_written()

  fun ref sync() ? => _backend.sync()?

  fun ref datasync() ? => _backend.datasync()?

  fun ref start_log_replay() => _backend.start_log_replay()

  fun ref write(): USize ? =>
    let bytes_written' = _backend.write()?
    match _file_length
    | let l: USize =>
      if bytes_written' >= l then
        if not _rotate_requested then
          _rotate_requested = true
          _event_log.start_rotation()
        end
      end
    end
    bytes_written'

  fun ref encode_entry(entry: LogEntry) => _backend.encode_entry(consume entry)

  fun ref rotate_file() ? =>
    // only do this if current backend has actually written anything
    if _backend.bytes_written() > 0 then
      // TODO This is a placeholder for recording that we're rotating
      // an EventLog backend file, which is a prototype quick hack for
      // keeping such state within an SimpleJournal collection thingie.
      let rotation_history = AsyncJournalledFile(FilePath(_auth, "TODO-EventLog-rotation-history.txt")?, _the_journal, _auth,
        _do_local_file_io)
      rotation_history.print("START of rotation: finished writing to " + _backend.get_path())

      // 1. sync/datasync the current backend to ensure everything is written
      _backend.sync()?
      _backend.datasync()?
      // 2. close the file by disposing the backend
      _backend.dispose()
      // 3. update _offset
      _offset = _offset + _backend.bytes_written().u64()
      // 4. open new backend with new file set to new offset.
      let p = _base_name + "-" + HexOffset(_offset) + _suffix
      let fp = FilePath(_base_dir, p)?
      let local_journal_filepath = FilePath(_base_dir, p + ".bin")?
      let local_journal = SimpleJournal(local_journal_filepath, false, _event_log)
      _backend = FileBackend(fp, _event_log, local_journal, _auth, _do_local_file_io)

      // TODO Part two of the log rotation hack.  Sync
      rotation_history.print("END of rotation: starting writing to " + _backend.get_path())
      rotation_history.sync() // TODO we want synchronous response
      rotation_history.dispose()
    end
    _rotate_requested = false

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



