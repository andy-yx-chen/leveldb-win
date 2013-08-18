#pragma once

#include <Windows.h>
#include <stdio.h>
#include <stdarg.h>
#include <time.h>
#include "leveldb\env.h"

namespace leveldb{
    class Win32Logger : public Logger{
    private:
        HANDLE _file;
    public:
        Win32Logger(HANDLE file) : _file(file){
        }

        virtual ~Win32Logger(){
            if(_file != INVALID_HANDLE_VALUE){
                CloseHandle(_file);
            }
        }

    public:
        virtual void Logv(const char* format, va_list ap){
            const DWORD thread_id = GetCurrentThreadId();
            int incremental_size = 1024;
            int current_size = 512, chars_written(0);
            SYSTEMTIME sys_time;
            char buffer[512];
            char* p = buffer;
            DWORD bytes_written = 0;
            DWORD total_bytes_written = 0;
            GetSystemTime(&sys_time);
            chars_written = _snprintf_s(p,  512, _TRUNCATE,
                "%04d/%02d/%02d-%02d:%02d:%02d.%03d %x ",
                sys_time.wYear,
                sys_time.wMonth,
                sys_time.wDay,
                sys_time.wHour,
                sys_time.wMinute,
                sys_time.wSecond,
                sys_time.wMilliseconds,
                thread_id);
            if(chars_written > 0){
                p += chars_written;
                chars_written = vsnprintf_s(p, 512 - chars_written, _TRUNCATE, format, ap);
            }

            //add a new line
            if (p == buffer || p[-1] != '\n') {
                *p++ = '\n';
            }

            while(total_bytes_written + buffer < p){
                WriteFile(this->_file, (LPCVOID) (buffer + total_bytes_written), p - buffer - total_bytes_written, &bytes_written, NULL);
                total_bytes_written += bytes_written;
            }
        }
    };
}
