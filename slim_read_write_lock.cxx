#include "slim_read_write_lock.h"
#include "win32_helper.h"
#include <algorithm>

slim_read_write_lock::slim_read_write_lock() {
    InitializeSRWLock(& this->_lock);
}

slim_read_write_lock::~slim_read_write_lock() {
}

void slim_read_write_lock::acquire_read_lock() throw() {
    AcquireSRWLockShared(&this->_lock);
}

void slim_read_write_lock::release_read_lock() throw() {
    ReleaseSRWLockShared(&this->_lock);
}

void slim_read_write_lock::acquire_write_lock() throw() {
    AcquireSRWLockExclusive(&this->_lock);
}

void slim_read_write_lock::release_write_lock() throw() {
    ReleaseSRWLockExclusive(&this->_lock);
}

std::string get_executable_dir(){
    char path[MAX_PATH];

    if(! GetModuleFileNameA(NULL, path, MAX_PATH)){
        return std::string("c:\\");
    }
    std::string exe_path(path);
    std::string::const_reverse_iterator last_slash_pos = std::find(exe_path.rbegin(), exe_path.rend(), '\\');
    if(last_slash_pos == exe_path.rend()){
        return std::string("c:\\");
    }
    return std::string(exe_path.begin(), std::string::const_iterator(last_slash_pos.base()));
}