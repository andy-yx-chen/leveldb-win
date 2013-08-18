#pragma
#include <hash_map>
#include <leveldb/db.h>
#include <boost/smart_ptr.hpp>
#include "slim_read_write_lock.h"

class db_manager {
public:
    db_manager();
    ~db_manager() throw();

public:
    boost::shared_ptr<leveldb::DB> open_db(const std::string& name);
    bool delete_db(const std::string& name);
    bool create_db(const std::string& name);
    std::vector<std::string> list_db() const;
    
public:
    typedef std::hash_map<std::string, boost::shared_ptr<leveldb::DB>> db_map;
    typedef std::pair<std::string, boost::shared_ptr<leveldb::DB>> db_item;

private:
    void load_options();
    void load_databases();
private:
    db_manager(const db_manager&);
    db_manager& operator = (const db_manager&);

private:
    db_map _databases;
    leveldb::Options* _options;
    leveldb::Cache* _cache;
    const leveldb::FilterPolicy* _filter_policy;
    mutable slim_read_write_lock _lock;
};