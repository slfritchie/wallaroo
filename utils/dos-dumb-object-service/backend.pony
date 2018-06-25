/****************************/
/* Copied from backend.pony */
/****************************/

use "files"

class AsyncJournalledFile
  let _file_path: String
  let _file: File
  let _journal: SimpleJournal2
  let _auth: AmbientAuth
  var _offset: USize
  let _do_local_file_io: Bool
  var _tag: USize = 1

  new create(filepath: FilePath, journal: SimpleJournal2,
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
  be async_io_ok(j: SimpleJournal2 tag, optag: USize)
  be async_io_error(j: SimpleJournal2 tag, optag: USize)

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

class SimpleJournalBackendRemote is SimpleJournalBackend
  let _rjc: RemoteJournalClient

  new create(rjc: RemoteJournalClient) =>
    _rjc = rjc

  fun ref be_dispose() =>
    _rjc.dispose()

  fun ref be_position(): USize =>
    666 // TODO

  fun ref be_writev(offset: USize, data: ByteSeqIter val, data_size: USize)
  : Bool
  =>
    // TODO offset sanity check
    // TODO offset update
    _D.d66("SimpleJournalBackendRemote: be_writev offset %d data_size %d\n",
       offset, data_size)
    _rjc.be_writev(offset, data, data_size)
    true

