@echo off
setlocal
REM Корневой каталог проекта
set "SCRIPT_DIR=%~dp0"
set "VENV_PATH=%SCRIPT_DIR%venv\Scripts\activate.bat"
set "PYTHON_SCRIPT=%SCRIPT_DIR%app_v2\admin_script.py"
set "REQUIREMENTS=%SCRIPT_DIR%requirements.txt"

ECHO Скрипт запускается из: %SCRIPT_DIR%

IF NOT EXIST "%VENV_PATH%" (
    ECHO Ошибка: Виртуальное окружение не найдено по пути %VENV_PATH%
    timeout /t 10
    exit /b 1
)

IF NOT EXIST "%PYTHON_SCRIPT%" (
    ECHO Ошибка: Python-скрипт не найден по пути %PYTHON_SCRIPT%
    timeout /t 10
    exit /b 1
)

ECHO Активация виртуального окружения...
call "%VENV_PATH%"

set PYTHONPATH=%SCRIPT_DIR%;%PYTHONPATH%

where python >nul 2>nul
IF ERRORLEVEL 1 (
    ECHO Ошибка: Python не найден (или не в PATH)
    timeout /t 10
    exit /b 1
)

ECHO Проверка зависимостей...
pip install -q -r "%REQUIREMENTS%"

ECHO Запуск скрипта %PYTHON_SCRIPT% ...
python "%PYTHON_SCRIPT%"

call deactivate

ECHO Скрипт завершён. Окно закроется через 10 секунд...
timeout /t 10
endlocal
exit /b 0
