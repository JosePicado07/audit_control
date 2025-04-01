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

:: Install primary data processing and analysis libraries
echo Installing data processing libraries...
python -m pip install pandas polars pyarrow dask numpy scipy scikit-learn --no-cache-dir
if errorlevel 1 goto data_error

:: Install Excel and reporting libraries
echo Installing Excel and reporting libraries...
python -m pip install openpyxl xlsxwriter xlrd --no-cache-dir
if errorlevel 1 goto reporting_error

:: Install GUI dependencies
echo Installing PyQt6 and related libraries...
python -m pip install PyQt6 PyQt6-tools PyQt6-Qt6 --no-cache-dir
if errorlevel 1 goto gui_error

echo Installing Pillow...
python -m pip install Pillow --no-cache-dir
if errorlevel 1 goto gui_error

:: Install logging and monitoring libraries
echo Installing logging and monitoring libraries...
python -m pip install loguru python-json-logger --no-cache-dir
if errorlevel 1 goto logging_error

:: Install additional useful libraries
echo Installing utility libraries...
python -m pip install tqdm rich --no-cache-dir
if errorlevel 1 goto utility_error

:: Final verification
echo.
echo Final Verification:
echo -----------------
python -c "import pandas; import polars; import pyarrow; import dask; import numpy; import scipy; import sklearn" >nul 2>&1
if errorlevel 1 echo ERROR: Data processing libraries not properly installed

python -c "import PyQt6; import PIL" >nul 2>&1
if errorlevel 1 echo ERROR: GUI libraries not properly installed

python -c "import openpyxl; import xlsxwriter" >nul 2>&1
if errorlevel 1 echo ERROR: Excel libraries not properly installed

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

:data_error
echo Error installing data processing libraries.
goto end

:reporting_error
echo Error installing Excel and reporting libraries.
goto end

:gui_error
echo Error installing GUI dependencies. Please check your internet connection and try again.
goto end

:logging_error
echo Error installing logging libraries.
goto end

:utility_error
echo Error installing utility libraries.
goto end

:end
echo.
echo Press any key to exit...
pause