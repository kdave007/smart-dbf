@echo off
REM Robust build script for Smart DBF
setlocal enabledelayedexpansion

echo ========================================
echo Smart DBF - Build Script
echo ========================================
echo.

echo [1/4] Checking environment...
python --version
if errorlevel 1 (
    echo ❌ Python not found in PATH
    pause
    exit /b 1
)

echo.
echo [2/4] Cleaning previous build...
timeout 1 >nul
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist

echo.
echo [3/4] Building executable...
echo Building from: src\test\test_controller.py
pyinstaller --clean --onefile --name "smart-dbf" src/test/test_controller.py

if errorlevel 1 (
    echo.
    echo ❌ BUILD FAILED!
    echo Possible issues:
    echo - Missing dependencies
    echo - Errors in Python code
    echo - Import issues
    echo.
    echo Run this to check dependencies:
    echo pip install -r requirements.txt
    pause
    exit /b 1
)

echo.
echo [4/4] Setting up distribution files...
if exist .env (
    copy .env dist\.env >nul
    echo ✅ .env copied
) else (
    echo ❌ .env not found
)

for %%i in (*.json) do (
    copy "%%i" "dist\%%i" >nul
    echo ✅ %%~nxi copied
)

if exist Advantage.Data.Provider.dll (
    copy Advantage.Data.Provider.dll dist\ >nul
    echo ✅ DLL copied
)

echo.
echo ========================================
echo ✅ BUILD SUCCESSFUL!
echo ========================================
echo.
echo 📊 Build Summary:
echo    Executable: dist\smart-dbf.exe
echo    Config: dist\.env
echo    JSON files: copied !JSON_COUNT!
echo.
echo 🚀 Ready to run!
echo.
pause