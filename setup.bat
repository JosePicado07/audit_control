@echo off
echo Initializing Audit Tool Setup...

:: Check Python installation
python --version >nul 2>&1
if errorlevel 1 goto install_python

:: Get Python path
for /f "tokens=*" %%i in ('where python') do set PYTHON_PATH=%%i
echo Python found at: %PYTHON_PATH%

:: Check Pip installation
python -m pip --version >nul 2>&1
if errorlevel 1 goto install_pip

echo.
echo Starting installation of dependencies...
echo.

:: Create necessary directories
if not exist "logs" mkdir logs
if not exist "reports" mkdir reports
if not exist "config" mkdir config

:: Install main dependencies
python -m pip install -e . --no-cache-dir
if errorlevel 1 goto pip_error

:: Install GUI dependencies
echo Installing customtkinter...
python -m pip install customtkinter --no-cache-dir
if errorlevel 1 goto gui_error

echo Installing Pillow...
python -m pip install Pillow --no-cache-dir
if errorlevel 1 goto gui_error

:: Final verification
echo.
echo Final Verification:
echo -----------------
python -c "import pandas" >nul 2>&1
if errorlevel 1 echo ERROR: pandas is not properly installed
python -c "import customtkinter" >nul 2>&1
if errorlevel 1 echo ERROR: customtkinter is not properly installed
python -c "import PIL" >nul 2>&1
if errorlevel 1 echo ERROR: Pillow is not properly installed

echo.
echo Setup completed successfully!
echo You can now run the application using run.bat
goto end

:install_python
echo Python is not installed. Please install Python 3.8 or later from https://www.python.org/downloads/
pause
exit /b

:install_pip
echo Pip is not installed correctly. Please reinstall Python with pip included.
pause
exit /b

:pip_error
echo Error installing main dependencies. Try running:
echo python -m pip install -e .
goto end

:gui_error
echo Error installing GUI dependencies. Please check your internet connection and try again.
goto end

:end
echo.
echo Press any key to exit...
pause