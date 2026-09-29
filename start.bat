@echo off
chcp 936 >nul
cd /d "%~dp0"
title 河海抢课器

rem ============================================================
rem  抢课轮询间隔，单位秒。这是「到点之后」的节奏。
rem  选课没开的时候脚本按 3 秒一轮慢跑，不受这里影响。
rem ============================================================
set INTERVAL=0.5

rem 等选课轮次放出来的查询间隔，单位秒。
rem 12:30 开抢，轮次一放出来就得抓住，所以是 1 秒一次。
rem 连续查询失败时脚本会自己退避拉长，不用你管。
set ROUND_EVERY=1

echo ============================================
echo   河海大学教务抢课器
echo   控制台: http://127.0.0.1:8899/
echo   轮询间隔 %INTERVAL%s
echo   等轮次间隔 %ROUND_EVERY%s
echo ============================================
echo.

where python >nul 2>nul
if errorlevel 1 (
    echo [错误] 没检测到 python 命令。
    echo        请先装 Python 3.8 以上，安装时勾选 Add Python to PATH。
    echo.
    pause
    exit /b 1
)

python -c "import requests, playwright" >nul 2>nul
if errorlevel 1 (
    echo [初始化] 首次运行，开始安装依赖，请稍候...
    echo.
    python -m pip install --upgrade pip
    python -m pip install -r requirements.txt
    if errorlevel 1 (
        echo.
        echo [提示] 直连安装失败，改用清华源重试...
        python -m pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
    )
    echo.
    echo [初始化] 安装浏览器内核...
    python -m playwright install chromium
    echo.
    echo [初始化] 完成。
    echo.
)

python xk_runner.py --port 8899 --interval %INTERVAL% --round-every %ROUND_EVERY% %*
echo.
echo 进程已退出。
pause