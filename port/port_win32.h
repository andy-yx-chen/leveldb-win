// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// This file contains the specification, but not the implementations,
// of the types/operations/etc. that should be defined by a platform
// specific port_<platform>.h file.  Use this file as a reference for
// how to port this package to a new platform.

#pragma once

#include <stdint.h>
#include <string>
#include "port/atomic_pointer.h"
#ifdef SNAPPY
#include <snappy.h>
#endif

namespace leveldb {
	namespace port {

		// TODO(jorlow): Many of these belong more in the environment class rather than
		//               here. We should try moving them and see if it affects perf.

		// The following boolean constant must be true on a little-endian machine
		// and false otherwise.
		static const bool kLittleEndian = true /* or some other expression */;

		// ------------------ Threading -------------------

		// A Mutex represents an exclusive lock.
		class Mutex {
		public:
			Mutex();
			~Mutex();

			// Lock the mutex.  Waits until other lockers have exited.
			// Will deadlock if the mutex is already locked by this thread.
			void Lock();

			// Unlock the mutex.
			// REQUIRES: This mutex was locked by this thread.
			void Unlock();

			// Optionally crash if this thread does not hold this mutex.
			// The implementation must be fast, especially if NDEBUG is
			// defined.  The implementation is allowed to skip all checks.
			void AssertHeld(){}

		private:
			CRITICAL_SECTION _lock;
			friend class CondVar;
		};

		class CondVar {
		public:
			explicit CondVar(Mutex* mu);
			~CondVar();

			// Atomically release *mu and block on this condition variable until
			// either a call to SignalAll(), or a call to Signal() that picks
			// this thread to wakeup.
			// REQUIRES: this thread holds *mu
			void Wait();

			// If there are some threads waiting, wake up at least one of them.
			void Signal();

			// Wake up all waiting threads.
			void SignalAll();
		private:
			Mutex* _mu;
			CONDITION_VARIABLE _cond_var;
		};

		// Thread-safe initialization.
		// Used as follows:
		//      static port::OnceType init_control = LEVELDB_ONCE_INIT;
		//      static void Initializer() { ... do something ...; }
		//      ...
		//      port::InitOnce(&init_control, &Initializer);
		typedef int OnceType;
#define LEVELDB_ONCE_INIT 0
		extern void InitOnce(port::OnceType*, void (*initializer)());

		// ------------------ Compression -------------------

		// Store the snappy compression of "input[0,input_length-1]" in *output.
		// Returns false if snappy is not supported by this port.
		inline bool Snappy_Compress(const char* input, size_t input_length,
			std::string* output)
		{
			output->resize(snappy::MaxCompressedLength(input_length));
			size_t out_length;
			snappy::RawCompress(input, input_length, &(*output)[0], &out_length);
			output->resize(out_length);
			return true;
		}

		// If input[0,input_length-1] looks like a valid snappy compressed
		// buffer, store the size of the uncompressed data in *result and
		// return true.  Else return false.
		inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
			size_t* result)
		{
			return snappy::GetUncompressedLength(input, length, result);
		}

		// Attempt to snappy uncompress input[0,input_length-1] into *output.
		// Returns true if successful, false if the input is invalid lightweight
		// compressed data.
		//
		// REQUIRES: at least the first "n" bytes of output[] must be writable
		// where "n" is the result of a successful call to
		// Snappy_GetUncompressedLength.
		inline bool Snappy_Uncompress(const char* input_data, size_t input_length,
			char* output)
		{
			return snappy::RawUncompress(input_data, input_length, output);
		}

		// ------------------ Miscellaneous -------------------

		// If heap profiling is not supported, returns false.
		// Else repeatedly calls (*func)(arg, data, n) and then returns true.
		// The concatenation of all "data[0,n-1]" fragments is the heap profile.
		inline bool GetHeapProfile(void (*func)(void*, const char*, int), void* arg)
		{
			return false;
		}

	}  // namespace port
}  // namespace leveldb

