
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include "db_service.h"
#include "dbmgr.h"

#include <boost/smart_ptr.hpp>
#include <boost/bind.hpp>
#include <iostream>
#include <vector>

#define HEADER_SIZE 8

#define COMMAND_LOGIN 1
#define COMMAND_OPEN 2
#define COMMAND_CLOSE 3
#define COMMAND_PUT 4
#define COMMAND_BATCH 5
#define COMMAND_GET 6
#define COMMAND_DELETE 7
#define COMMAND_LIST 8
#define COMMAND_CREATE 9

#define RESULT_OK 0
#define RESULT_IO_ERROR 501
#define RESULT_UN_AUTH 401
#define RESULT_DATA_ERROR 400
#define RESULT_NO_DB 402
#define RESULT_CREAT_FAILED 502
#define RESULT_NO_DB_SELECTED 403
#define RESULT_DB_ERROR 503
#define RESULT_BAD_COMMAND 404
#define RESULT_NOT_FOUND 405

using boost::asio::ip::tcp;

db_manager dbmgr;

inline int read_int(const char* buffer){
  int val = 0;
  val |= (int)(unsigned char)buffer[0];
  val |= ((int)(unsigned char)buffer[1]) << 8;
  val |= ((int)(unsigned char)buffer[2]) << 16;
  val |= ((int)(unsigned char)buffer[3]) << 24;
  return val;
}

inline boost::shared_array<char> write_int(int val){
  char* buffer = new char[4];
  buffer[0] = (char)(val & 255);
  buffer[1] = (char)((val >> 8) & 255);
  buffer[2] = (char)((val >> 16) & 255);
  buffer[3] = (char)((val >> 24) & 255);
  return boost::shared_array<char>(buffer);
}

class db_session;

class db_command : public boost::enable_shared_from_this<db_command> {
public:
  typedef boost::shared_ptr<db_command> pointer;

public:
  db_command(const boost::shared_ptr<db_session>& session)
    : _session(session), _buffer(new char[4]), _buf_size(4){
  }

public:
  virtual void execute(const boost::shared_array<char>& data, int data_size);

protected:

  const char* data() const{
    return _buffer.get();
  }

  virtual void complete();

  tcp::socket& socket();

  size_t buffer_size() const{
    return _buf_size;
  }

  boost::shared_ptr<db_session>& session(){
    return _session;
  }

  virtual void process_data() = 0;

private:
  db_command(const db_command&);
  db_command& operator = (const db_command&);

private:
  boost::shared_ptr<db_session> _session;
  boost::shared_array<char> _buffer;
  size_t _buf_size;
};

class db_session : public boost::enable_shared_from_this<db_session>{
public:
  typedef boost::shared_ptr<db_session> pointer;

private:
  db_session(boost::asio::io_service& io) : _socket(io), _current_db(), _command_data(), _bytes_read(0), _data_size(0){
  }

private:
  void data_read(const boost::system::error_code& error, size_t bytes_transferred);
  void read_complete();

private:
  static boost::shared_ptr<db_command> create_command(int command, const db_session::pointer& session);

public:
  static pointer create(boost::asio::io_service& io){
    return pointer(new db_session(io));
  }

public:
  boost::shared_ptr<leveldb::DB> current_db(){
    return _current_db;
  }

  tcp::socket& socket(){
    return _socket;
  }

  void set_db(const boost::shared_ptr<leveldb::DB>& db){
    _current_db = db;
  }

  void start();

private:
  tcp::socket _socket;
  boost::shared_ptr<leveldb::DB> _current_db;
  boost::shared_array<char> _command_data;
  int _bytes_read;
  int _data_size;
  char _header[HEADER_SIZE];
};

void db_command::execute(const boost::shared_array<char>& data, int data_size){
  _buf_size = data_size;
  _buffer = data;
  this->process_data();
}

void db_command::complete(){
  _session->start();
}

tcp::socket& db_command::socket(){
  return _session->socket();
}

void db_session::start() {
  pointer self = shared_from_this();
  boost::asio::async_read(_socket, boost::asio::buffer(_header, HEADER_SIZE), [this,self](const boost::system::error_code& error, size_t /*bytes_transffered*/){
    if(!error){
      _data_size = read_int(_header + 4);
      _bytes_read = 0;
      if(_data_size <= 0){
        this->read_complete();
        return;
      }
      this->_command_data.reset(new char[_data_size]);
      boost::asio::async_read(_socket, boost::asio::buffer(_command_data.get(), _data_size), boost::bind(&db_session::data_read, self,
        boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred));
    }
    // else, read header error, drop the connection
  });
}

void db_session::data_read(const boost::system::error_code& error, size_t bytes_transferred){
  pointer self = shared_from_this();
  if(!error){
    this->read_complete();
  }
}

void db_session::read_complete(){
  int command = read_int(_header);
  pointer self = shared_from_this();
  db_command::pointer db_cmd = db_session::create_command(command, self);
  if(db_cmd){
    db_cmd->execute(_command_data, _data_size);
  }else{
    boost::shared_array<char> buffer(new char[8]);
    char len_buf[] = {0, 0, 0, 0};
    boost::shared_array<char> result(write_int(RESULT_BAD_COMMAND));
    memcpy(buffer.get(), result.get(), 4);
    memcpy(buffer.get() + 4, len_buf, 4);
    boost::asio::async_write(_socket, boost::asio::buffer(buffer.get(), 8), [this, self](const boost::system::error_code& error, size_t /*bytes_transferred*/){
      if(!error){
        this->start();
      }else{ // io error drop the socket
        this->_socket.close();
      }
    });
  }
}

class login_command : public db_command{
public:
  login_command(const boost::shared_ptr<db_session>& session) :
    db_command(session){
  }

protected:
  virtual void process_data();
};


void login_command::process_data(){
  // for now, we don't have user database, just return OK to client
  pointer self = shared_from_this();
  boost::shared_array<char> buffer(new char[8]);
  boost::shared_array<char> result (write_int(RESULT_OK));
  char len[] = {0, 0, 0, 0};
  memcpy(buffer.get(), result.get(), 4);
  memcpy(buffer.get() + 4, len, 4);
  boost::asio::async_write(socket(), boost::asio::buffer(buffer.get(), 8), [this, self, buffer](const boost::system::error_code& error, size_t /*bytes_transferred*/){
    if(!error){
      this->complete();
    }else{
      // io error, close out and stop here
      socket().close();
    }
  });
}

class open_command : public db_command{
public:
  open_command(const boost::shared_ptr<db_session>& session)
    :db_command(session){
  }

protected:
  virtual void process_data();

private:
  virtual int do_open(const std::string& db_name);
};

void open_command::process_data(){
  boost::shared_array<char> buffer(new char[8]);
  char len_buf[] = {0, 0, 0, 0};
  if(buffer_size() < 4){
    // error, invalid command data
    boost::shared_array<char> result(write_int(RESULT_DATA_ERROR));
    memcpy(buffer.get(), result.get(), 4);
    memcpy(buffer.get() + 4, len_buf, 4);
    pointer self = shared_from_this();
    boost::asio::async_write(socket(), boost::asio::buffer(buffer.get(), 8), [self, this, buffer](const boost::system::error_code& error, size_t){
      if(!error){
        this->complete();
      }else{
        socket().close();
      }
    });
    return;
  }
  const char* buf = data();
  std::string db_name(buf, buf + buffer_size());
  int status = do_open(db_name);
  boost::shared_array<char> result(write_int(status));
  memcpy(buffer.get(), result.get(), 4);
  memcpy(buffer.get() + 4, len_buf, 4);
  pointer self = shared_from_this();
  boost::asio::async_write(socket(), boost::asio::buffer(buffer.get(), 8), [this, self, buffer](const boost::system::error_code& error, size_t){
    if(!error){
      this->complete();
    }else{
      //we done, we could not recover from the error
      socket().close();
    }
  });
}

int open_command::do_open(const std::string& db_name){
  boost::shared_ptr<leveldb::DB> db = dbmgr.open_db(db_name);
  if(db){
    session()->set_db(db);
    return RESULT_OK;
  }
  return RESULT_NO_DB;
}

class create_command : public open_command{
public:
  explicit create_command(const boost::shared_ptr<db_session>& session) : 
  open_command(session){
  }

private:
  virtual int do_open(const std::string& db_name);
};

int create_command::do_open(const std::string& db_name){
  if(dbmgr.create_db(db_name)){
    return RESULT_OK;
  }
  return RESULT_CREAT_FAILED;
}

class tx_command : public db_command{
public:
  tx_command(const boost::shared_ptr<db_session>& session)
    : db_command(session){
  }

protected:
  virtual void process_data() = 0;

  void response(int status);
};

void tx_command::response(int status){
  boost::shared_array<char> buffer(new char[8]);
  char buf_len[] = {0, 0, 0, 0};
  boost::shared_array<char> result(write_int(status));
  memcpy(buffer.get(), result.get(), 4);
  memcpy(buffer.get() + 4, buf_len, 4);
  pointer self = shared_from_this();
  boost::asio::async_write(socket(), boost::asio::buffer(buffer.get(), 8), [this, self, buffer](const boost::system::error_code& error, size_t){
    if(!error){
      this->complete();
    }else{
      socket().close();
    }
  });
}

class put_command : public tx_command{
public:
  put_command(const boost::shared_ptr<db_session>& session) 
    : tx_command(session){
  }

protected:
  virtual void process_data();
};

void put_command::process_data(){
  if(!session()->current_db()){
    response(RESULT_NO_DB_SELECTED);
    return;
  }
  int buf_size = buffer_size();
  const char* buf = data();
  if(buf_size < 8){
    response(RESULT_DATA_ERROR);
    return;
  }
  int key_size = read_int(buf);
  int value_size = read_int(buf + 4);
  buf_size -= 8;
  if(buf_size < key_size + value_size || key_size < 0 || value_size < 0){
    response(RESULT_DATA_ERROR);
    return;
  }
  leveldb::Slice key(buf + 8, key_size);
  leveldb::Slice value(buf + 8 + key_size, value_size);
  boost::shared_ptr<leveldb::DB> db = session()->current_db();
  leveldb::Status status = db->Put(leveldb::WriteOptions(), key, value);
  if(!status.ok()){
    response(RESULT_DB_ERROR);
    return;
  }
  response(RESULT_OK);
  return;
}

class delete_command : public tx_command{
public:
  delete_command(const boost::shared_ptr<db_session>& session) 
    : tx_command(session)
  {
  }

protected:
  virtual void process_data();
};

void delete_command::process_data(){
  if(!session()->current_db()){
    response(RESULT_NO_DB_SELECTED);
    return;
  }

  int buf_size = buffer_size();
  const char* buf = data();
  if(buf_size <= 0){
    response(RESULT_DATA_ERROR);
    return;
  }

  leveldb::Slice key(buf, buf_size);
  boost::shared_ptr<leveldb::DB> db = session()->current_db();
  leveldb::Status status = db->Delete(leveldb::WriteOptions(), key);
  if(status.ok()){
    response(RESULT_OK);
    return;
  }

  if(status.IsNotFound()){
    response(RESULT_NOT_FOUND);
    return;
  }

  response(RESULT_DB_ERROR);
}

class read_command : public tx_command{
public:
  read_command(const boost::shared_ptr<db_session>& session)
    : tx_command(session){
  }

protected:
  virtual void process_data();
};

void read_command::process_data(){
  if(!session()->current_db()){
    response(RESULT_NO_DB_SELECTED);
    return;
  }
  int buf_size = buffer_size();
  const char* buf = data();
  if(buf_size <= 0){
    response(RESULT_DATA_ERROR);
    return;
  }
  leveldb::Slice key(buf, buf_size);
  boost::shared_ptr<leveldb::DB> db = session()->current_db();
  std::string value;
  leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &value);

  if(!status.ok()){
    if(status.IsNotFound()){
      response(RESULT_NOT_FOUND);
    }else{
      response(RESULT_DB_ERROR);
    }
    return;
  }
  boost::shared_array<char> buffer(new char[8 + value.size()]);
  boost::shared_array<char> result(write_int(RESULT_OK));
  boost::shared_array<char> buf_len(write_int((int)value.size()));
  memcpy(buffer.get(), result.get(), 4);
  memcpy(buffer.get() + 4, buf_len.get(), 4);
  memcpy(buffer.get() + 8, value.data(), value.size());
  pointer self = shared_from_this();
  boost::asio::async_write(socket(), boost::asio::buffer(buffer.get(), 8 + value.size()), [self, this, buffer](const boost::system::error_code& error, size_t){
    if(!error){
      this->complete();
    }else{
      socket().close();
    }
  });

}

class batch_command : public tx_command{
public:
  batch_command(const boost::shared_ptr<db_session>& session)
    : tx_command(session){
  }

protected:
  virtual void process_data();
};

void batch_command::process_data(){
  if(!session()->current_db()){
    response(RESULT_NO_DB_SELECTED);
    return;
  }
  int buf_size = buffer_size();
  const char* buf = data();
  if(buf_size <= 0){
    response(RESULT_DATA_ERROR);
    return;
  }
  int items_count = read_int(buf);
  buf += 4;
  buf_size -= 4;
  if(items_count <= 0){
    response(RESULT_DATA_ERROR);
    return;
  }
  leveldb::WriteBatch batch;
  leveldb::Slice key;
  leveldb::Slice value;
  int key_size(0), value_size(0);
  while(items_count > 0){
    int command = read_int(buf);
    buf += 4;
    buf_size -= 4;
    switch(command){
    case COMMAND_PUT:
      key_size = read_int(buf);
      buf += 4;
      buf_size -= 4;
      value_size = read_int(buf);
      buf += 4;
      buf_size -= 4;
      if(key_size + value_size > buf_size || key_size <= 0 || value_size <= 0){
        response(RESULT_DATA_ERROR);
        return;
      }
      key = leveldb::Slice(buf, key_size);
      buf += key_size;
      buf_size -= key_size;
      value = leveldb::Slice(buf, value_size);
      buf += value_size;
      buf_size -= value_size;
      batch.Put(key, value);
      break;
    case COMMAND_DELETE:
      key_size = read_int(buf);
      buf += 4;
      buf_size -= 4;
      if(key_size <= 0 || buf_size < key_size){
        response(RESULT_DATA_ERROR);
        return;
      }
      key = leveldb::Slice(buf, key_size);
      batch.Delete(key);
      break;
    default:
      response(RESULT_BAD_COMMAND);
      return;
    }
    --items_count;
  }//end while
  boost::shared_ptr<leveldb::DB> db = session()->current_db();
  leveldb::Status s = db->Write(leveldb::WriteOptions(), &batch);
  if(s.ok()){
    response(RESULT_OK);
    return;
  }
  response(RESULT_DB_ERROR);
  return;
}

class close_command : public tx_command{
public:
  close_command(const boost::shared_ptr<db_session>& session)
    : tx_command(session){
  }

protected:
  virtual void process_data();
};

void close_command::process_data(){
  session()->set_db(boost::shared_ptr<leveldb::DB>(NULL));
  response(RESULT_OK);
  return;
}

class list_command : public tx_command{
public:
  list_command(const boost::shared_ptr<db_session>& session)
    : tx_command(session){
  }

protected:
  virtual void process_data();
};

void list_command::process_data(){
  response(RESULT_OK);
}

boost::shared_ptr<db_command> db_session::create_command(int command, const db_session::pointer& session) {
  switch (command)
  {
  case COMMAND_BATCH:
    return boost::shared_ptr<db_command>(new batch_command(session));
  case COMMAND_CLOSE:
    return boost::shared_ptr<db_command>(new close_command(session));
  case COMMAND_CREATE:
    return boost::shared_ptr<db_command>(new ::create_command(session));
  case COMMAND_GET:
    return boost::shared_ptr<db_command>(new read_command(session));
  case COMMAND_LIST:
    return boost::shared_ptr<db_command>(new list_command(session));
  case COMMAND_LOGIN:
    return boost::shared_ptr<db_command>(new login_command(session));
  case COMMAND_OPEN:
    return boost::shared_ptr<db_command>(new open_command(session));
  case COMMAND_PUT:
    return boost::shared_ptr<db_command>(new put_command(session));
  case COMMAND_DELETE:
    return boost::shared_ptr<db_command>(new delete_command(session));
  default:
    return boost::shared_ptr<db_command>();
    break;
  }
}

class db_tcp_server {
public:
  db_tcp_server(boost::asio::io_service& io) : _acceptor(io, tcp::endpoint(tcp::v4(), 4406)){
  }

public:

  void stop(){
    boost::system::error_code error;
    _acceptor.close(error);
  }

  void start(){
    db_session::pointer session = db_session::create(_acceptor.get_io_service());
    _acceptor.async_accept(session->socket(), [this, session](const boost::system::error_code& error){
      if(!error){
        session->start();
      }
      start();
    });
  }
private:
  tcp::acceptor _acceptor;
};

class db_service_impl{
public:
  db_service_impl() : _io_service(), _tcp_server(), _worker_threads(){
  }

public:
  void start(){
    SYSTEM_INFO sys_info = {0};
    GetSystemInfo(&sys_info);
    if(_tcp_server){
      _tcp_server->stop();
    }
    _tcp_server.reset(new db_tcp_server(_io_service));
    _tcp_server->start();
    //start io threads
    for(DWORD counter = 0; counter < sys_info.dwNumberOfProcessors; ++ counter){
      _worker_threads.push_back(boost::shared_ptr<boost::thread>(new boost::thread(boost::bind(&boost::asio::io_service::run, &_io_service))));
    }
  }

  void stop(){
    if(_tcp_server){
      _tcp_server->stop();
    }
    _io_service.stop();
    std::for_each(_worker_threads.begin(), _worker_threads.end(), [](boost::shared_ptr<boost::thread>& thread){
      thread->join();
    });
  }
private:
  boost::asio::io_service _io_service;
  boost::shared_ptr<db_tcp_server> _tcp_server;
  std::vector<boost::shared_ptr<boost::thread>> _worker_threads;
};

db_service::db_service() : _impl(new db_service_impl)
{
}

db_service::~db_service()
{
}

bool db_service::start()
{
  _impl->start();
  return true;
}

void db_service::stop()
{
  _impl->stop();
}
