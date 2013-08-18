#pragma once
#include <boost/smart_ptr.hpp>

extern void service_mode(int argc, char** argv);

class db_service_impl;

class db_service {
public:
    db_service();
    ~db_service();
public:
    bool start();
    void stop();

private:
    db_service(const db_service&);
    db_service& operator= (const db_service&);

private:
    boost::shared_ptr<db_service_impl> _impl;
};