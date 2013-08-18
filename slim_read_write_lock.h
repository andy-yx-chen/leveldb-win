#pragma once
#include <Windows.h>

class slim_read_write_lock {
public:
    slim_read_write_lock();
    ~slim_read_write_lock();

public:
    void acquire_read_lock() throw();
    void release_read_lock() throw();
    void acquire_write_lock() throw();
    void release_write_lock() throw();

private:
    SRWLOCK _lock;
};

class srw_lock_guard {
public:
    enum lock_type{
        read,
        write
    };

public:
    srw_lock_guard(slim_read_write_lock& lock, lock_type ltype) : _lock(lock), _ltype(ltype){
        switch (_ltype)
        {
        case srw_lock_guard::read:
            _lock.acquire_read_lock();
            break;
        case srw_lock_guard::write:
            _lock.acquire_write_lock();
            break;
        default:
            break;
        }
    }

    ~srw_lock_guard() throw(){
        switch (_ltype)
        {
        case srw_lock_guard::read:
            _lock.release_read_lock();
            break;
        case srw_lock_guard::write:
            _lock.release_write_lock();
            break;
        default:
            break;
        }
    }

private:
    slim_read_write_lock& _lock;
    lock_type _ltype;
};