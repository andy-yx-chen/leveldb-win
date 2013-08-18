========================================================================
    LevelDB on Windows Project
========================================================================

This project is used to port Google LevelDB project to Windows platform.
We make it as a Win32 service which is listening on port 4406 (tcp)

This project has dependency of Boost Asio and with boost 1.54, asio has
a bug with Windows IOCP as below,
in file asio/detail/impl/win_iocp_io_service.ipp, at line 286, you will 
see something like below,
::PostQueuedCompletionStatus(iocp_.handle, 0, 0, op)
You should change it to 
::PostQueuedCompletionStatus(iocp_.handle, 0, overlapped_contains_result, op)

After fix this, the LevelDb service will run well on Windows platform.

I have also enable Snappy compression for this project.


