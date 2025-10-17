@echo off
REM Build script for Smart DBF executable

echo ========================================
echo Smart DBF - Build Script
echo ========================================
echo.

echo [1/4] Checking dependencies...
python -c "import pyinstaller" >nul 2>&1
if errorlevel 1 (
    echo Installing PyInstaller...
    pip install pyinstaller
)

echo.
echo [2/4] Cleaning previous build...
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist

echo.
echo [3/4] Building executable with ALL utils JSON files included...

REM Construir comando PyInstaller que incluye TODOS los archivos JSON de src/utils
pyinstaller --onefile --name "smart-dbf" ^
  --add-data "src/utils/data_tables_schemas.json;src/utils" ^
  --add-data "src/utils/mappings.json;src/utils" ^
  --add-data "src/utils/rules.json;src/utils" ^
  --add-data "src/utils/sql_identifiers.json;src/utils" ^
  src/test/test_controller.py

if errorlevel 1 (
    echo ❌ BUILD FAILED!
    pause
    exit /b 1
)

echo.
echo [4/4] COPYING EXTERNAL CONFIGURATION FILES ONLY...

REM Copiar SOLO los archivos que deben ser externos
if exist .env (
    copy .env dist\.env
    echo ✅ Copied .env to dist folder (EXTERNAL)
) else (
    echo ❌ ERROR: .env file not found!
)

REM Copiar SOLO el archivo venue configurado en .env
for /f "tokens=2 delims==" %%i in ('findstr "VENUE_FILE_NAME" .env') do (
    set VENUE_FILE=%%~i
    set VENUE_FILE=!VENUE_FILE:"=!
)

if exist "!VENUE_FILE!" (
    copy "!VENUE_FILE!" "dist\!VENUE_FILE!"
    echo ✅ Copied !VENUE_FILE! to dist folder (EXTERNAL)
) else (
    echo ❌ ERROR: Venue file !VENUE_FILE! not found!
)

REM Copiar DLL si existe
if exist Advantage.Data.Provider.dll (
    copy Advantage.Data.Provider.dll dist\Advantage.Data.Provider.dll
    echo ✅ Copied Advantage.Data.Provider.dll to dist folder
)

echo.
echo ========================================
echo ✅ BUILD COMPLETED SUCCESSFULLY!
echo ========================================
echo.
echo 📁 Distribution structure:
echo.
echo INTERNAL (incluidos en EXE):
echo    data_tables_schemas.json
echo    mappings.json  
echo    rules.json
echo    sql_identifiers.json
echo.
echo EXTERNAL (fuera del EXE):
echo    .env
echo    !VENUE_FILE!
echo    Advantage.Data.Provider.dll (si aplica)
echo.
echo 🚀 Executable: dist\smart-dbf.exe
echo.
pause