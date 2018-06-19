@echo off

if not exist "%TIDAL_HOME%\bin\runserver.cmd" echo Please set the TIDAL_HOME variable in your environment! & EXIT /B 1

call "%TIDAL_HOME%\bin\runserver.cmd" tidal.server.ServerStartup %*