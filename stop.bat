@echo off
REM Stop all services for the Stock Market Streaming System

echo ==================================
echo Stopping Stock Market Streaming System
echo ==================================

echo.
echo Stopping Docker services...
docker-compose down

echo.
echo ==================================
echo All services stopped!
echo ==================================
echo.
echo To stop and remove all data, run:
echo   docker-compose down -v
echo.
pause
