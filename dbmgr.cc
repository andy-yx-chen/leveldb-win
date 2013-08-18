#include "dbmgr.h"
#include "win32_helper.h"
#include <leveldb/cache.h>
#include <leveldb/filter_policy.h>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

db_manager::db_manager() : _databases(), _options(NULL), _cache(NULL), _filter_policy(NULL), _lock() {
  this->load_options();
  this->load_databases();
}

db_manager::~db_manager(){
  if(_options != NULL){
    delete _options;
  }

  if(_cache != NULL){
    delete _cache;
  }

  if(_filter_policy != NULL){
    delete _filter_policy;
  }
}

void db_manager::load_options() {
  using boost::property_tree::ptree;
  ptree settings_tree;
  _options = new leveldb::Options;
  _options->create_if_missing = true;
  try{
    std::string config_file(std::move(get_executable_dir() + "leveldb.xml"));
    boost::property_tree::read_xml(config_file, settings_tree);
    int cache_size = settings_tree.get<int>("leveldb.cache_size", -1);
    int write_buffer_size = settings_tree.get<int>("leveldb.write_buffer_size", 0);
    int max_open_files = settings_tree.get<int>("leveldb.max_open_files", 0);
    int bloom_bits = settings_tree.get<int>("leveldb.bloom_bits", -1);
    if( cache_size >= 0){
      _cache = leveldb::NewLRUCache((size_t)cache_size);
      _options->block_cache = _cache;
    }

    if(bloom_bits >= 0){
      _filter_policy = leveldb::NewBloomFilterPolicy(bloom_bits);
      _options->filter_policy = _filter_policy;
    }

    if(write_buffer_size > 0){
      _options->write_buffer_size = write_buffer_size;
    }

    if(max_open_files > 0){
      _options->max_open_files = max_open_files;
    }
  }catch(...){
  }
}

void db_manager::load_databases() {
  std::string exe_folder(std::move(get_executable_dir()));
  WIN32_FIND_DATAA find_data = {0};
  std::string filter(std::move(exe_folder + "*"));
  HANDLE find_handle = FindFirstFileA(filter.c_str(), &find_data);
  if(find_handle != INVALID_HANDLE_VALUE){
    srw_lock_guard lock_guard(_lock, srw_lock_guard::lock_type::write);
    do{
      if((find_data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 && 
        (strcmp(find_data.cFileName, ".") != 0 && strcmp(find_data.cFileName, "..") != 0)){
          leveldb::DB* db(NULL);
          std::string db_folder(std::move(exe_folder + find_data.cFileName));
          leveldb::Status status = leveldb::DB::Open(*_options, db_folder.c_str(), &db);
          if(status.ok()){
            _databases.insert(db_item(std::move(std::string(find_data.cFileName)), boost::shared_ptr<leveldb::DB>(db)));
          }
      }
    }while(FindNextFileA(find_handle, &find_data) != FALSE);
  }
}

boost::shared_ptr<leveldb::DB> db_manager::open_db(const std::string& dbname) {
  srw_lock_guard lock_guard(_lock, srw_lock_guard::lock_type::read);
  db_map::iterator result = _databases.find(dbname);
  if(result != _databases.end()){
    return result->second;
  }
  return boost::shared_ptr<leveldb::DB>();
}

bool db_manager::delete_db(const std::string& dbname){
  {
    srw_lock_guard lock_guard(_lock, srw_lock_guard::lock_type::write);
    db_map::iterator result = _databases.find(dbname);
    if(result != _databases.end()){
      _databases.erase(result);
    }
  }
  // delete the folder by rmdir /s /q
  std::string db_folder(std::move(get_executable_dir() + dbname));
  std::string cmd(std::move("cmd /c \"rmdir /s /q \"\"" + db_folder +  "\"\"\""));
  char* cmd_line = new char[cmd.length() + 20];
  strcpy_s(cmd_line, cmd.length() + 20, cmd.c_str());
  STARTUPINFOA si = {0};
  si.cb = sizeof(si);
  PROCESS_INFORMATION pi = {0};
  if(CreateProcessA(NULL, cmd_line, NULL, NULL, FALSE, CREATE_NO_WINDOW, NULL, NULL, &si, &pi) == TRUE){
    CloseHandle(pi.hProcess);
    CloseHandle(pi.hThread);
  }
  delete[] cmd_line;
  return false;
}

bool db_manager::create_db(const std::string& dbname) {
  {
    srw_lock_guard lock_guard(_lock, srw_lock_guard::lock_type::read);
    db_map::iterator result = _databases.find(dbname);
    if(result != _databases.end()){
      return false;
    }
  }
  std::string db_folder(std::move(get_executable_dir() + dbname));
  leveldb::DB* db = NULL;
  leveldb::Status status = leveldb::DB::Open(*_options, db_folder.c_str(), &db);
  if(status.ok())
  {
    srw_lock_guard lock_guard(_lock, srw_lock_guard::lock_type::write);
    _databases.insert(db_item(dbname, boost::shared_ptr<leveldb::DB>(db)));
    return true;
  }
  return false;
}

std::vector<std::string> db_manager::list_db() const {
  srw_lock_guard lock_guard(_lock, srw_lock_guard::lock_type::read);
  std::vector<std::string> db_list;
  std::for_each(_databases.begin(), _databases.end(), [&db_list](const db_item& item) -> void {
    db_list.push_back(item.first);
  });
  return std::move(db_list);
}