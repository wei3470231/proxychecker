@echo off
chcp 65001 >nul
echo [Build] Starting Compilation for All Platforms...

if not exist "build" mkdir build

set GO_BIN=go

echo [Build] Using Go: %GO_BIN%
%GO_BIN% version

echo.
echo ---------------------------------------------------
echo [1/3] Windows x64...
set GOOS=windows
set GOARCH=amd64
%GO_BIN% build -o build\proxychecker.exe .
if %errorlevel% neq 0 (
    echo [ERROR] Windows build failed!
    pause
    exit /b %errorlevel%
)
echo [OK] build\proxychecker.exe

echo.
echo ---------------------------------------------------
echo [2/3] Linux ARM64 (Termux/Android)...
set GOOS=linux
set GOARCH=arm64
%GO_BIN% build -o build\proxychecker-linux-arm64-v8a .
if %errorlevel% neq 0 (
    echo [ERROR] Linux ARM64 build failed!
    pause
    exit /b %errorlevel%
)
echo [OK] build\proxychecker-linux-arm64-v8a

echo.
echo ---------------------------------------------------
echo [3/3] Linux ARMv7...
set GOOS=linux
set GOARCH=arm
set GOARM=7
%GO_BIN% build -o build\proxychecker-linux-arm-v7a .
if %errorlevel% neq 0 (
    echo [ERROR] Linux ARMv7 build failed!
    pause
    exit /b %errorlevel%
)
echo [OK] build\proxychecker-linux-arm-v7a

echo.
echo ===================================================
echo [Done] All builds completed!
echo ===================================================
pause
