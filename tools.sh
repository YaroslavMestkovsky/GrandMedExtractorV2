#!/bin/bash

# Пути относительно текущей директории
VENV_RELATIVE_PATH="venv/Scripts/Activate"  # Для Windows (venv\Scripts\activate)
PYTHON_SCRIPT="app/run_tools.py"                  # Python-файл
REQUIREMENTS="requirements.txt"                  # Файл зависимостей

# Полный абсолютный путь к текущей директории (для отладки)
CURRENT_DIR=$(pwd)
echo "Текущая директория: $CURRENT_DIR"

# Проверка наличия виртуального окружения
if [ ! -f "$VENV_RELATIVE_PATH" ]; then
    echo "Ошибка: Виртуальное окружение не найдено по пути $VENV_RELATIVE_PATH"
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
source "$VENV_RELATIVE_PATH"

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
