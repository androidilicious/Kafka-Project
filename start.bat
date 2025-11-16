@echo off
REM Start all services for the Stock Market Streaming System

echo ==================================
echo Starting Stock Market Streaming System
echo ==================================

REM Start Docker services
echo.
echo 1. Starting Docker services...
docker-compose up -d

echo.
echo 2. Waiting for services to be ready...
timeout /t 10 /nobreak >nul

REM Check if services are running
echo.
echo 3. Checking service status...
docker-compose ps

echo.
echo ==================================
echo Services are ready!
echo ==================================
echo.
echo Next steps:
echo 1. Open a new terminal and run: python producer.py
echo 2. Open another terminal and run: python consumer.py
echo 3. Open another terminal and run: streamlit run dashboard.py
echo 4. (Optional) Run aggregator: python aggregator.py
echo.
echo Dashboard will be available at: http://localhost:8501
echo Flink UI available at: http://localhost:8081
echo.
pause
