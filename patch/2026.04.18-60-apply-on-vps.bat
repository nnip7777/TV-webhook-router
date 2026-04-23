@echo off
setlocal

set PATCH_VERSION=2026.04.18-60
set VPS_HOST=root@72.56.246.125

echo Applying patch %PATCH_VERSION% on VPS...
ssh %VPS_HOST% "cd /opt/webhook-router && python3 scripts/apply_patch.py patch/%PATCH_VERSION% /opt/webhook-router && systemctl restart webhook-router && sleep 2 && curl -fsS http://127.0.0.1:8787/healthz"
if errorlevel 1 goto :fail

echo Patch applied successfully.
goto :eof

:fail
echo Apply failed.
exit /b 1
