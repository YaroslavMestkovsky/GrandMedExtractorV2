#!/bin/bash

# Определяем директорию скрипта (корень проекта)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Пути на основе директории скрипта
VENV_PATH="$SCRIPT_DIR/venv/Scripts/activate"   # Для Git-Bash/WSL
PYTHON_SCRIPT="$SCRIPT_DIR/app_v3/script.py"       # Python-файл
REQUIREMENTS="$SCRIPT_DIR/requirements.txt"     # Файл зависимостей

# Полный путь к директории скрипта (для отладки)
echo "Директория скрипта: $SCRIPT_DIR"

# Проверка наличия виртуального окружения
if [ ! -f "$VENV_PATH" ]; then
    echo "Ошибка: Виртуальное окружение не найдено по пути $VENV_PATH"
    sleep 10  # Задержка перед закрытием
    exit 1
fi

# Проверка наличия Python-файла
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "Ошибка: Python-файл не найден по пути $PYTHON_SCRIPT"
    sleep 10  # Задержка перед закрытием
    exit 1
fi

# Активация виртуального окружения
echo "Активация виртуального окружения..."
source "$VENV_PATH"

# Добавление корня проекта в PYTHONPATH (для импортов)
export PYTHONPATH="$SCRIPT_DIR:$SCRIPT_DIR/app_v3:${PYTHONPATH}"

# Проверка, что Python доступен
if ! command -v python &> /dev/null; then
    echo "Ошибка: Python не найден в системе или виртуальном окружении"
    sleep 10  # Задержка перед закрытием
    exit 1
fi

# Проверка и установка зависимостей
echo "Проверка зависимостей..."
pip install -q -r "$REQUIREMENTS"

# Запуск Python-файла
echo "Запуск Python-файла $PYTHON_SCRIPT..."
python "$PYTHON_SCRIPT"

# Деактивация окружения
deactivate

# Задержка перед закрытием (10 секунд)
echo "Скрипт завершён. Окно закроется через 10 секунд..."
sleep 10
exit 0
