#include "port\port_win32.h"
#include <Windows.h>
#include <vector>

#define PORT_WIN32_ONCE_EXECUTED 1

namespace leveldb{
	namespace port{

		Mutex::Mutex(){
			memset((void*)&this->_lock, 0, sizeof(CRITICAL_SECTION));
			InitializeCriticalSection(&this->_lock);
		}

		Mutex::~Mutex(){
			DeleteCriticalSection(&this->_lock);
			memset((void*)&this->_lock, 0, sizeof(CRITICAL_SECTION));
		}

		void Mutex::Lock(){
			EnterCriticalSection(&this->_lock);
		}

		void Mutex::Unlock(){
			LeaveCriticalSection(&this->_lock);
		}

		CondVar::CondVar(Mutex* mu)
			:_mu(mu){
				memset((void*) &this->_cond_var, 0, sizeof(CONDITION_VARIABLE));
				InitializeConditionVariable(&this->_cond_var);
		}

		CondVar::~CondVar(){
			//do nothing
		}

		void CondVar::Wait(){
			SleepConditionVariableCS(&this->_cond_var, &this->_mu->_lock, INFINITE);
		}

		void CondVar::Signal(){
			WakeConditionVariable(&this->_cond_var);
		}

		void CondVar::SignalAll(){
			WakeAllConditionVariable(&this->_cond_var);
		}

		BOOL CALLBACK InitHandleFunction(PINIT_ONCE init_once, PVOID param, PVOID* context){
			void (*initializer)();
			initializer = (void (*)())param;
			initializer();
			return TRUE;
		}

		std::vector<void(*)()> init_functions;
		INIT_ONCE g_init_once = INIT_ONCE_STATIC_INIT;
		SRWLOCK g_vector_srw_lock = {0};

		BOOL CALLBACK InitSRWLock(PINIT_ONCE once, PVOID params, PVOID* context){
			InitializeSRWLock(&g_vector_srw_lock);
			return TRUE;
		}

		void InitOnce(OnceType* once, void (*initializer)()){
			//initialize the SRW lock once
			InitOnceExecuteOnce(&g_init_once, &InitSRWLock, NULL, NULL);
			if(*once != LEVELDB_ONCE_INIT){
				return;
			}

			bool executed(false);
			AcquireSRWLockShared(&g_vector_srw_lock);

			if(std::find(init_functions.begin(), init_functions.end(), initializer) != init_functions.end()){
				executed = true;
			}

			ReleaseSRWLockShared(&g_vector_srw_lock);

			if(!executed){
				AcquireSRWLockExclusive(&g_vector_srw_lock);
				if(std::find(init_functions.begin(), init_functions.end(), initializer) == init_functions.end()){
					(*initializer)();
					init_functions.push_back(initializer);
				}

				ReleaseSRWLockExclusive(&g_vector_srw_lock);
			}

			*once = PORT_WIN32_ONCE_EXECUTED;
		}
	}
}