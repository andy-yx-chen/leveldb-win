#include "leveldb\env.h"
#include "win32_logger.h"
#include <Windows.h>

#if defined(DeleteFile)
#undef DeleteFile
#endif

namespace leveldb{
    class Win32FileLock : public FileLock{
    private:
        HANDLE _file;
        std::string _name;
    public:
        Win32FileLock(HANDLE file, const std::string& name): _file(file), _name(name){
        }

    public:
        HANDLE get_file() { return _file; };
        std::string& get_name() {return _name;}
    };

    std::string last_error_as_string(){
        DWORD error = GetLastError();
        char buffer[20];
        sprintf_s(buffer, "%x", error);
        return std::string(buffer);
    }

    class Win32SequentialFile : public SequentialFile{
    private:
        HANDLE _file_handle;
        std::string _file_name;

    public:
        Win32SequentialFile(const std::string& file_name, HANDLE file_handle)
            : _file_handle(file_handle), _file_name(file_name){
        }

        virtual ~Win32SequentialFile(){
            if(this->_file_handle != INVALID_HANDLE_VALUE){
                CloseHandle(this->_file_handle);
            }
        }

    public:
        virtual Status Read(size_t n, Slice* result, char* scratch) ;

        virtual Status Skip(uint64_t n){
            LONG distance_low(0L), distance_high(0L);
            DWORD result(INVALID_SET_FILE_POINTER);
            distance_low = (LONG)n;
            distance_high = (LONG)(n >> 32);
            result = SetFilePointer(this->_file_handle, distance_low, &distance_high, FILE_CURRENT);
            if(result == INVALID_SET_FILE_POINTER){
                return Status::IOError(this->_file_name, last_error_as_string());
            }
            return Status::OK();
        }
    };

    Status Win32SequentialFile::Read(size_t n, Slice* result, char* scratch){
        DWORD bytes_read(0);
        BOOL is_success = ReadFile(this->_file_handle, (LPVOID)scratch, n, &bytes_read, NULL);
        if(is_success != TRUE){
            return Status::IOError(this->_file_name, last_error_as_string());
        }
        *result = Slice(scratch, bytes_read);
        return Status::OK();
    }

    class Win32RandomAccessFile : public RandomAccessFile{
    private:
        HANDLE _file_handle;
        LPVOID _map_address;
        HANDLE _map_handle;
        uint64_t _file_size;

    public:
        Win32RandomAccessFile(HANDLE file_handle) : _file_handle(file_handle), _map_address(NULL), _map_handle(NULL), _file_size(0){
            if(this->_file_handle != INVALID_HANDLE_VALUE){
                DWORD file_size_high(0), file_size_low(0);
                SIZE_T size_to_map(0);
                file_size_low = GetFileSize(this->_file_handle, &file_size_high);
                this->_file_size = file_size_low;
                this->_map_handle = CreateFileMappingA(this->_file_handle, NULL, PAGE_READONLY, file_size_high, file_size_low, NULL);
                if(this->_map_handle != NULL){
                    this->_map_address = MapViewOfFile(this->_map_handle, FILE_MAP_READ, 0, 0, file_size_low);
                }
            }
        }

        virtual Status Read(uint64_t offset, size_t n, Slice* result,
            char* scratch) const;

        virtual ~Win32RandomAccessFile(){
            if(this->_map_address != NULL){
                UnmapViewOfFile(this->_map_address);
                this->_map_address = NULL;
            }

            if(this->_map_handle != NULL){
                CloseHandle(this->_map_handle);
                this->_map_handle = NULL;
            }

            if(this->_file_handle != INVALID_HANDLE_VALUE){
                CloseHandle(this->_file_handle);
                this->_file_handle = INVALID_HANDLE_VALUE;
            }
        }
    };

    Status Win32RandomAccessFile::Read(uint64_t offset, size_t n, Slice* result, char* scratch) const{
        if(offset + n > this->_file_size){
            Status::IOError("EOF reached");
        }
        memcpy((void*)scratch, (void*)(((char*) this->_map_address) + offset), n);
        *result = Slice(scratch, n);
        return Status::OK();
    }

    class Win32WritableFile : public WritableFile{
    private:
        HANDLE _file_handle;
    public:
        Win32WritableFile(HANDLE file_handle) : _file_handle(file_handle){
        }

        virtual ~Win32WritableFile(){
            if(this->_file_handle != INVALID_HANDLE_VALUE){
                CloseHandle(this->_file_handle);
                this->_file_handle = INVALID_HANDLE_VALUE;
            }
        }

    public:
        virtual Status Append(const Slice& data);

        virtual Status Close(){
            if(this->_file_handle != INVALID_HANDLE_VALUE){
                CloseHandle(this->_file_handle);
                this->_file_handle = INVALID_HANDLE_VALUE;
                return Status::OK();
            }
            return Status::IOError("invalid file found");
        }

        virtual Status Flush(){
            FlushFileBuffers(this->_file_handle);
            return Status::OK();
        }

        virtual Status Sync(){
            return Status::OK();
        }
    };

    Status Win32WritableFile::Append(const Slice& data){
        size_t bytes_to_write = data.size();
        DWORD bytes_written(0), total_bytes_written(0);
        while(bytes_to_write > 0){
          if(!WriteFile(this->_file_handle, (LPVOID)(data.data() + total_bytes_written), bytes_to_write, &bytes_written, NULL)){
            return Status::IOError("failed to write data to file", last_error_as_string());
          }
          total_bytes_written += bytes_written;
          bytes_to_write -= bytes_written;
        }
        return Status::OK();
    }
#define MAP_INCREMENTAL_SIZE 1<<20
#define MAP_LIMIT_SIZE 1<<31

    class Win32MappedViewWritableFile : public WritableFile{
    private:
        HANDLE _file_handle;
        HANDLE _map_handle;
        char* _view_base;
        char* _view_cursor;
        DWORD _view_incremental_size;
        DWORD _view_size;
        DWORD _map_size;
        DWORD _granularity;
        DWORD _view_offset;
        DWORD _file_offset;

    public:
        Win32MappedViewWritableFile(HANDLE file_handle):
            _file_handle(file_handle), _map_handle(NULL), _view_base(NULL), _view_cursor(NULL), _view_incremental_size(0), _view_size(0),
            _map_size(MAP_INCREMENTAL_SIZE), _granularity(0), _view_offset(0), _file_offset(0)
        {
            SYSTEM_INFO sys_info = {0};
            GetSystemInfo(&sys_info);
            this->_granularity = sys_info.dwAllocationGranularity;
            DWORD page_size = sys_info.dwPageSize;
            this->_view_incremental_size = ((page_size + this->_granularity - 1) /  this->_granularity) * this->_granularity;
            this->_view_size = this->_view_incremental_size;
            this->_file_offset = GetFileSize(this->_file_handle, NULL);
            this->_view_offset = (this->_file_offset / this->_granularity) * this->_granularity;
            this->_map_size = (1 + (this->_view_size + this->_file_offset) / this->_map_size) * this->_map_size;
            MapRegion();
        }

        ~Win32MappedViewWritableFile(){
            Cleanup();
        }
    public:
        virtual Status Close(){
            Cleanup();
            return Status::OK();
        }

        virtual Status Flush(){
            return Status::OK();
        }

        virtual Status Sync(){
            if(0 != FlushViewOfFile(this->_view_base, this->_view_cursor - this->_view_base)){
                return Status::OK();
            }
            return Status::IOError("failed to flush content to file", last_error_as_string());
        }

        Status Append(const Slice& data){
            //make sure we have enough space to hold the data
            while(this->_view_cursor - this->_view_base + data.size() > this->_view_size){
                UnmapViewOfFile(this->_view_base);
                this->_view_base = NULL;
                DWORD new_view_offset = (this->_file_offset / this->_granularity) * this->_granularity;
                if(new_view_offset == this->_view_offset){
                    this->_view_size += this->_view_incremental_size;
                }else{
                    this->_view_offset = new_view_offset;
                }
                if(this->_view_offset + this->_view_size > this->_map_size){//we need to recreate the map region
                    //we reach the limit, exit
                    if(this->_map_size >= MAP_LIMIT_SIZE){
                        CloseHandle(this->_map_handle);
                        this->_map_handle = NULL;
                        TruncateFile();
                        return Status::IOError("file size has reached its limitation");
                    }
                    while(this->_view_offset + this->_view_size > this->_map_size){
                        this->_map_size += MAP_INCREMENTAL_SIZE;
                    }
                    CloseHandle(this->_map_handle);
                    this->_map_handle = NULL;
                    TruncateFile();
                }
                if(!MapRegion().ok()){
                    return Status::IOError("failed to write", last_error_as_string());
                }
            }
            //now we can write data
            memcpy(this->_view_cursor, data.data(), data.size());
            this->_view_cursor += data.size();
            this->_file_offset += data.size();
            return Status::OK();
        }
    private:
        Status MapRegion(){
            if(this->_map_handle == NULL){
                this->_map_handle = CreateFileMappingA(this->_file_handle, NULL, PAGE_READWRITE, 0, this->_map_size, NULL);
            }
            if(this->_map_handle != NULL){
                this->_view_base = (char*)MapViewOfFile(this->_map_handle, FILE_MAP_READ | FILE_MAP_WRITE, 0, this->_view_offset, this->_view_size);
                if(this->_view_base != NULL){
                    this->_view_cursor = this->_view_base + (this->_view_offset == 0 ? this->_file_offset : this->_file_offset % this->_view_offset);
                    return Status::OK();
                }else{
                    CloseHandle(this->_map_handle);
                    this->_map_handle = NULL;
                    TruncateFile();
                    return Status::IOError("failed to map a view for file", last_error_as_string());
                }
            }
            return Status::IOError("failed to create a file mapping", last_error_as_string());
        }

        void Cleanup(){
            if(this->_view_base != NULL){
                UnmapViewOfFile(this->_view_base);
                this->_view_base = NULL;
            }
            if(this->_map_handle != NULL){
                CloseHandle(this->_map_handle);
                this->_map_handle = NULL;
                TruncateFile();
            }
            if(this->_file_handle != INVALID_HANDLE_VALUE){
                CloseHandle(this->_file_handle);
                this->_file_handle = INVALID_HANDLE_VALUE;
            }
        }

        Status TruncateFile(){
            FILE_END_OF_FILE_INFO eof_info = {0};
            eof_info.EndOfFile.HighPart = 0;
            eof_info.EndOfFile.LowPart = this->_file_offset;
            if(0 == SetFileInformationByHandle(this->_file_handle, FileEndOfFileInfo, &eof_info, sizeof(FILE_END_OF_FILE_INFO))){
                return Status::IOError("failed to set file size", last_error_as_string());
            }
            return Status::OK();
        }
    };

    class Win32Env : public Env{
    public:
        ~Win32Env(){}
        // Create a brand new sequentially-readable file with the specified name.
        // On success, stores a pointer to the new file in *result and returns OK.
        // On failure stores NULL in *result and returns non-OK.  If the file does
        // not exist, returns a non-OK status.
        //
        // The returned file will only be accessed by one thread at a time.
        virtual Status NewSequentialFile(const std::string& fname,
                                         SequentialFile** result){
          HANDLE file_handle = CreateFileA(fname.c_str(), GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
          if(file_handle == INVALID_HANDLE_VALUE){
            return Status::IOError("failed to open file", last_error_as_string());
          }
          *result = new Win32SequentialFile(fname, file_handle);
          return Status::OK();
        }

        // Create a brand new random access read-only file with the
        // specified name.  On success, stores a pointer to the new file in
        // *result and returns OK.  On failure stores NULL in *result and
        // returns non-OK.  If the file does not exist, returns a non-OK
        // status.
        //
        // The returned file may be concurrently accessed by multiple threads.
        virtual Status NewRandomAccessFile(const std::string& fname,
                                           RandomAccessFile** result){
          HANDLE file_handle = CreateFileA(fname.c_str(), GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
          if(file_handle == INVALID_HANDLE_VALUE){
            return Status::IOError("failed to open file", last_error_as_string());
          }
          *result = new Win32RandomAccessFile(file_handle);
          return Status::OK();
        }

        // Create an object that writes to a new file with the specified
        // name.  Deletes any existing file with the same name and creates a
        // new file.  On success, stores a pointer to the new file in
        // *result and returns OK.  On failure stores NULL in *result and
        // returns non-OK.
        //
        // The returned file will only be accessed by one thread at a time.
        virtual Status NewWritableFile(const std::string& fname,
                                       WritableFile** result){
          HANDLE file_handle = CreateFileA(fname.c_str(), GENERIC_WRITE | GENERIC_READ, 0, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
          if(file_handle == INVALID_HANDLE_VALUE){
            return Status::IOError("failed to open file", last_error_as_string());
          }
          *result = new Win32MappedViewWritableFile(file_handle);
          return Status::OK();
        }

        // Returns true iff the named file exists.
        virtual bool FileExists(const std::string& fname){
          HANDLE file_handle = CreateFileA(fname.c_str(), GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
          if(file_handle == INVALID_HANDLE_VALUE){
            return false;
          }
          CloseHandle(file_handle);
          return true;
        }

        // Store in *result the names of the children of the specified directory.
        // The names are relative to "dir".
        // Original contents of *results are dropped.
        virtual Status GetChildren(const std::string& dir,
                                   std::vector<std::string>* result){
          WIN32_FIND_DATAA find_data;
          HANDLE find_handle;
          std::string filter(dir);
          filter += "\\*";
          find_handle = FindFirstFileA(filter.c_str(), &find_data);
          if(find_handle == INVALID_HANDLE_VALUE){
            return Status::IOError("directory does not exist", last_error_as_string());
          }
          result->clear();
          do{
            result->push_back(find_data.cFileName);
          }while(FindNextFileA(find_handle, &find_data));
          FindClose(find_handle);
          return Status::OK();
        }

        // Delete the named file.
        virtual Status DeleteFile(const std::string& fname){
          if(DeleteFileA(fname.c_str())){
            return Status::OK();
          }
          return Status::IOError("could not delete file", last_error_as_string());
        }

        // Create the specified directory.
        virtual Status CreateDir(const std::string& dirname){
          if(CreateDirectoryA(dirname.c_str(), NULL)){
            return Status::OK();
          }
          return Status::IOError("could not create directory", last_error_as_string());
        }

        // Delete the specified directory.
        virtual Status DeleteDir(const std::string& dirname){
          if(RemoveDirectoryA(dirname.c_str())){
            return Status::OK();
          }
          return Status::IOError("couldn't remove directory", last_error_as_string());
        }

        // Store the size of fname in *file_size.
        virtual Status GetFileSize(const std::string& fname, uint64_t* file_size){
          HANDLE file_handle = CreateFileA(fname.c_str(), GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
          if(file_handle == INVALID_HANDLE_VALUE){
            return Status::IOError("cannot open file", last_error_as_string());
          }
          *file_size = ::GetFileSize(file_handle, NULL);
          CloseHandle(file_handle);
          return Status::OK();
        }

        // Rename file src to target.
        virtual Status RenameFile(const std::string& src,
                                  const std::string& target){
          if(MoveFileA(src.c_str(), target.c_str())){
            return Status::OK();
          }
          if(ERROR_ALREADY_EXISTS == GetLastError()){
              if(DeleteFileA(target.c_str())){
                  if(MoveFileA(src.c_str(), target.c_str())){
                    return Status::OK();
                  }
              }
          }
          return Status::IOError("cannot rename file", last_error_as_string());
        }
        // Lock the specified file.  Used to prevent concurrent access to
        // the same db by multiple processes.  On failure, stores NULL in
        // *lock and returns non-OK.
        //
        // On success, stores a pointer to the object that represents the
        // acquired lock in *lock and returns OK.  The caller should call
        // UnlockFile(*lock) to release the lock.  If the process exits,
        // the lock will be automatically released.
        //
        // If somebody else already holds the lock, finishes immediately
        // with a failure.  I.e., this call does not wait for existing locks
        // to go away.
        //
        // May create the named file if it does not already exist.
        virtual Status LockFile(const std::string& fname, FileLock** lock){
          HANDLE file_handle = CreateFileA(fname.c_str(), GENERIC_WRITE, 0, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
          if(file_handle == INVALID_HANDLE_VALUE){
            return Status::IOError("failed to open file", last_error_as_string());
          }
          *lock = new Win32FileLock(file_handle, fname);
          return Status::OK();
        }

        // Release the lock acquired by a previous successful call to LockFile.
        // REQUIRES: lock was returned by a successful LockFile() call
        // REQUIRES: lock has not already been unlocked.
        virtual Status UnlockFile(FileLock* lock){
            CloseHandle(reinterpret_cast<Win32FileLock*>(lock)->get_file());
          return Status::OK();
        }

        // Arrange to run "(*function)(arg)" once in a background thread.
        //
        // "function" may run in an unspecified thread.  Multiple functions
        // added to the same Env may run concurrently in different threads.
        // I.e., the caller may not assume that background work items are
        // serialized.
        virtual void Schedule(
            void (*function)(void* arg),
            void* arg);

        // Start a new thread, invoking "function(arg)" within the new thread.
        // When "function(arg)" returns, the thread will be destroyed.
        virtual void StartThread(void (*function)(void* arg), void* arg);

        // *path is set to a temporary directory that can be used for testing. It may
        // or many not have just been created. The directory may or may not differ
        // between runs of the same process, but subsequent calls will return the
        // same directory.
        virtual Status GetTestDirectory(std::string* path);

        // Create and return a log file for storing informational messages.
        virtual Status NewLogger(const std::string& fname, Logger** result){
          HANDLE file_handle = CreateFileA(fname.c_str(), GENERIC_ALL, 0, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
          if(file_handle == INVALID_HANDLE_VALUE){
            return Status::IOError("cannot create log file", last_error_as_string());
          }
          *result = new Win32Logger(file_handle);
          return Status::OK();
        }

        // Returns the number of micro-seconds since some fixed point in time. Only
        // useful for computing deltas of time.
      virtual uint64_t NowMicros(){
        SYSTEMTIME sys_time = {0};
        ULARGE_INTEGER time_value = {0};
        FILETIME file_time = {0};
        GetSystemTime(&sys_time);
        SystemTimeToFileTime(&sys_time, &file_time);
        time_value.HighPart = file_time.dwHighDateTime;
        time_value.LowPart = file_time.dwLowDateTime;
        return (uint64_t)(time_value.QuadPart / 10000);
      }

        // Sleep/delay the thread for the perscribed number of micro-seconds.
      virtual void SleepForMicroseconds(int micros){
        Sleep(micros);
      }

    };

  Status Win32Env::GetTestDirectory(std::string* path){
    char buff[256];
    DWORD buffer_size = ExpandEnvironmentStringsA("%TEMP%", buff, 256);
    if(buffer_size == 0){
      return Status::IOError("No temp folder", last_error_as_string());
    }else if(buffer_size > 256){
      char* buffer = new char[buffer_size];
      ExpandEnvironmentStringsA("%TEMP%", buffer, buffer_size);
      *path = buffer;
      delete[] buffer;
    }else{
      *path = buff;
    }
    return Status::OK();
  }

  DWORD WINAPI ThreadEntry(LPVOID parameters){
    void** pointers = (void**)parameters;
    void (*entry_point)(void*);
    entry_point = (void (*)(void*))pointers[0];
    void* args = pointers[1];
    (*entry_point)(args);
    delete[] pointers;
    return 0;
  }

  void Win32Env::StartThread(void (*function)(void* arg), void* arg){
    void** args = new void*[2];
    args[0] = (void*) function;
    args[1] = arg;
    HANDLE thread_handle = CreateThread(NULL, 0, &ThreadEntry, (LPVOID)args, 0, NULL);
    if(thread_handle != NULL){
      CloseHandle(thread_handle);
    }
  }

  void Win32Env::Schedule(void (*function)(void* arg), void* arg){
    void** args = new void*[2];
    args[0] = (void*) function;
    args[1] = arg;
    QueueUserWorkItem(&ThreadEntry, (LPVOID)args, WT_EXECUTEDEFAULT);
  }

  static Env* default_env;
  INIT_ONCE env_init_once = INIT_ONCE_STATIC_INIT;

  BOOL CALLBACK InitEnv(PINIT_ONCE once, PVOID params, PVOID* context){
    default_env = new Win32Env();
    return TRUE;
  }

  Env* Env::Default(){
    InitOnceExecuteOnce(&env_init_once, &InitEnv, NULL, NULL);
    return default_env;
  }
}
