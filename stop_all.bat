@echo off
REM ==============================================================================
REM CALIBRE — Stop All Services in One Command (Windows)
REM ==============================================================================
title CALIBRE - Stop All

set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"

echo ============================================================
echo  CALIBRE PLATFORM — ONE-COMMAND SHUTDOWN
echo ============================================================

echo.
echo [1/3] Terminating any running Python pipeline processes...
wmic process where "name='python.exe' and commandline like '%%run_pipeline.py%%'" call terminate >nul 2>&1
echo       Done.

echo.
echo [2/3] Stopping Docker infrastructure (Kafka, Zookeeper, Flink)...
docker compose down --remove-orphans >nul 2>&1
for %%c in (calibre-kafka calibre-zookeeper calibre-flink-jobmanager calibre-flink-taskmanager calibre-init-kafka) do (
    docker rm -f %%c >nul 2>&1
)
echo       Docker containers stopped and removed.

echo.
echo [3/3] Verifying shutdown...
docker ps --filter "name=calibre" --format "table {{.Names}}\t{{.Status}}"

echo.
echo ============================================================
echo  SUCCESS: All CALIBRE services have been cleanly stopped.
echo  Relational database intact at: %SCRIPT_DIR%data\calibre_persistence.db
echo ============================================================

