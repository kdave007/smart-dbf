@echo off
REM Clean build artifacts

echo Cleaning build artifacts...

if exist build (
    rmdir /s /q build
    echo Removed build folder
)

if exist dist (
    rmdir /s /q dist
    echo Removed dist folder
)

if exist __pycache__ (
    rmdir /s /q __pycache__
    echo Removed __pycache__ folder
)

REM Clean all __pycache__ folders recursively
for /d /r . %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d"

REM Clean .pyc files
del /s /q *.pyc 2>nul

echo.
echo Clean completed!
pause
