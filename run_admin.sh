#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_PATH="$SCRIPT_DIR/venv/Scripts/activate"    # Windows (cmd/bash scripts)
PYTHON_SCRIPT="$SCRIPT_DIR/app_v2/admin_script.py"
REQUIREMENTS="$SCRIPT_DIR/requirements.txt"

export LANG=C.UTF-8
export PYTHONIOENCODING=utf-8
# Экспортируем PYTHONPATH на корень проекта для корректных импортов
export PYTHONPATH="$SCRIPT_DIR"
echo "PYTHONPATH: $PYTHONPATH"

echo "Директория скрипта: $SCRIPT_DIR"

if [ ! -f "$VENV_PATH" ]; then
    echo "Ошибка: Виртуальное окружение не найдено по пути $VENV_PATH"
    sleep 10
    exit 1
fi

if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "Ошибка: Python-файл не найден по пути $PYTHON_SCRIPT"
    sleep 10
    exit 1
fi

echo "Активация виртуального окружения..."
source "$VENV_PATH"

if ! command -v python &> /dev/null; then
    echo "Ошибка: Python не найден"
    sleep 10
    exit 1
fi

echo "Проверка зависимостей..."
pip install -q -r "$REQUIREMENTS"

echo "Запуск Python-файла $PYTHON_SCRIPT..."
PYTHONIOENCODING=utf-8 python "$PYTHON_SCRIPT"

deactivate

echo "Скрипт завершён. Окно закроется через 10 секунд..."
sleep 10
exit 0
