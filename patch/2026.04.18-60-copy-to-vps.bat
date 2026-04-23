@echo off
setlocal

set PATCH_VERSION=2026.04.18-60
set LOCAL_PATCH_DIR=/Users/nik/strategy/webhook-router/patch/%PATCH_VERSION%
set VPS_TARGET=root@72.56.246.125:/opt/webhook-router/patch/%PATCH_VERSION%/

echo Copying patch %PATCH_VERSION% to VPS...
rsync -av "%LOCAL_PATCH_DIR%/" "%VPS_TARGET%"
if errorlevel 1 goto :fail

echo Patch copied successfully.
goto :eof

:fail
echo Copy failed.
exit /b 1
