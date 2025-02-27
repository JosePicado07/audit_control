@echo off
echo Starting Audit Tool...

:: Check Python installation
python --version >nul 2>&1
if errorlevel 1 (
   echo Python is not installed or not added to PATH. Please run setup.bat first.
   pause
   exit /b
)

:: Display Python interpreter info
echo Python installation found:
python --version
where python

:: Check critical dependencies
echo.
echo Checking dependencies...

python -c "import pandas" >nul 2>&1
if errorlevel 1 (
    echo Error: pandas module not found. Please run setup.bat first.
    pause
    exit /b
)

python -c "import customtkinter" >nul 2>&1
if errorlevel 1 (
    echo Error: customtkinter module not found. Please run setup.bat first.
    pause
    exit /b
)

:: Check directories exist
if not exist "logs" mkdir logs
if not exist "reports" mkdir reports
if not exist "config" (
    echo Error: config directory not found. Please run setup.bat first.
    pause
    exit /b
)

:: Run the application
echo.
echo All dependencies found. Starting application...
echo.
python main.py

if errorlevel 1 (
    echo.
    echo An error occurred while running the application.
    echo Please check the logs for more information.
)

pause