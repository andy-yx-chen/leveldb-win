#include "db_service.h"
#include <Windows.h>
#include <iostream>
#include "leveldbrc.h"
#include "dbmgr.h"

#pragma comment(lib, "advapi32.lib")

char* g_service_name = "leveldbsvc";
char* g_service_display_name = "level db service";

SERVICE_STATUS g_service_status;
SERVICE_STATUS_HANDLE g_service_status_handle;
HANDLE g_stop_event;

db_service g_svc;

VOID svc_install(void);

VOID WINAPI service_control_handler(DWORD);

VOID WINAPI service_main(DWORD, LPSTR *);

void report_svc_status(DWORD, DWORD, DWORD);

void svc_report_event(LPSTR);

void service_mode(int argc, char** argv) {
#ifdef _DEBUG
  g_svc.start();
  std::cout << "press any key to stop the service" << std::endl;
  system("pause");
  g_svc.stop();
#else 
    if(argc > 1 && lstrcmpiA(argv[1], "--install") == 0) {
        svc_install();
        return;
    }

    SERVICE_TABLE_ENTRYA dispatch_table[]  = {
        {g_service_name, (LPSERVICE_MAIN_FUNCTIONA) &service_main},
        {NULL, NULL}
    };

    if(!StartServiceCtrlDispatcherA(dispatch_table)){
        svc_report_event("StartServiceCtrlDispatcher");
    }
#endif
}

void svc_install() {
    char file_path[MAX_PATH];
    if(!GetModuleFileNameA(NULL, file_path, MAX_PATH)) {
        std::cerr << "Cannot install service as error " << GetLastError() << std::endl;
        return;
    }

    SC_HANDLE scm_handle = OpenSCManagerA(NULL, NULL, SC_MANAGER_ALL_ACCESS);

    if(NULL == scm_handle) {
        std::cerr << "Failed to open the SCM handle as " << GetLastError() << std::endl;
        return;
    }

    SC_HANDLE svc_handle = CreateServiceA(
        scm_handle,
        g_service_name,
        g_service_display_name,
        SERVICE_ALL_ACCESS,
        SERVICE_WIN32_OWN_PROCESS,
        SERVICE_AUTO_START,
        SERVICE_ERROR_NORMAL,
        file_path,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL);

    if( NULL == svc_handle ) {
        std::cerr << "Failed to create a service as " << GetLastError() << std::endl;
        return;
    }

    std::cout << "service " << g_service_name << " has installed successfully." << std::endl;
    CloseServiceHandle(svc_handle);
    CloseServiceHandle(scm_handle);
}

VOID WINAPI service_main(DWORD argc, LPSTR* argv) {
    g_service_status_handle = RegisterServiceCtrlHandlerA(g_service_name, (LPHANDLER_FUNCTION) &service_control_handler);

    if( !g_service_status_handle ){
        svc_report_event("RegisterServiceCtrlHandler");
        return;
    }
    
    g_service_status.dwServiceSpecificExitCode = 0;
    g_service_status.dwServiceType = SERVICE_WIN32_OWN_PROCESS;

    report_svc_status(SERVICE_START_PENDING, NO_ERROR, 3000);
    // initialize db service
    if(! g_svc.start()){
        svc_report_event("StartService");
        report_svc_status(SERVICE_STOPPED, NO_ERROR, 0);
        return;
    }

    g_stop_event = CreateEventA(NULL, TRUE, FALSE, NULL);
    if(g_stop_event == NULL) {
        report_svc_status(SERVICE_STOPPED, NO_ERROR, 0);
        return;
    }
    report_svc_status(SERVICE_RUNNING, NO_ERROR, 0);
    WaitForSingleObject(g_stop_event, INFINITE);
    // stop the service
    g_svc.stop();

    report_svc_status(SERVICE_STOPPED, NO_ERROR, 0);
    CloseHandle(g_stop_event);
    return;
}

void report_svc_status(DWORD status, DWORD error, DWORD latency) {
    static DWORD check_point = 1;
    g_service_status.dwCurrentState = status;
    g_service_status.dwWin32ExitCode;
    g_service_status.dwWaitHint = latency;

    if(status == SERVICE_START_PENDING) {
        g_service_status.dwControlsAccepted = 0;
    }else{
        g_service_status.dwControlsAccepted = SERVICE_ACCEPT_STOP;
    }

    if(status == SERVICE_RUNNING ||
        status == SERVICE_STOPPED) {
            g_service_status.dwCheckPoint = 0;
    }else {
        g_service_status.dwCheckPoint = check_point ++;
    }
    SetServiceStatus(g_service_status_handle, &g_service_status);
}

void svc_report_event(LPSTR function) {
    HANDLE source_handle = RegisterEventSourceA(NULL, g_service_name);

    if(NULL != source_handle) {
        LPCSTR msg_strings[2];
        char message[80];
        sprintf_s(message, "%s failed with %x", function, GetLastError());
        msg_strings[0] = function;
        msg_strings[1] = message;
        ReportEventA(source_handle, EVENTLOG_ERROR_TYPE, 0, SVC_ERROR, NULL, 2, 0, msg_strings, NULL);
        DeregisterEventSource(source_handle);
    }
}

VOID WINAPI service_control_handler(DWORD control) {
    switch (control)
    {
    case SERVICE_CONTROL_STOP:
        report_svc_status(SERVICE_STOP_PENDING, NO_ERROR, 1000);
        SetEvent(g_stop_event);
    default:
        break;
    }
}