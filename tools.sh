#!/bin/bash

# Пути относительно текущей директории
REQUIREMENTS="requirements.txt"                  # Файл зависимостей
# Главный модуль для запуска (используем модульный запуск, чтобы работали импорты)
MAIN_MODULE="app.tools.tools"

# Полный абсолютный путь к текущей директории (для отладки)
CURRENT_DIR=$(pwd)
echo "Текущая директория: $CURRENT_DIR"

# Определяем исполняемый Python из venv (без активации окружения)
VENV_PY_WIN="venv/ScriptS/python.exe"
VENV_PY_WIN_ALT="venv/Scripts/python.exe"
VENV_PY_NIX="venv/bin/python"

if [ -x "$VENV_PY_WIN" ]; then
    PY_EXEC="$VENV_PY_WIN"
elif [ -x "$VENV_PY_WIN_ALT" ]; then
    PY_EXEC="$VENV_PY_WIN_ALT"
elif [ -x "$VENV_PY_NIX" ]; then
    PY_EXEC="$VENV_PY_NIX"
else
    echo "Ошибка: Не найден исполняемый Python в виртуальном окружении (venv)."
    echo "Ожидались: $VENV_PY_WIN или $VENV_PY_WIN_ALT или $VENV_PY_NIX"
    sleep 10
    exit 1
fi

# Устанавливаем PYTHONPATH на корень проекта, чтобы работали импорты вида `from app...`
export PYTHONPATH="$CURRENT_DIR"

# Проверка и установка зависимостей
echo "Проверка зависимостей..."
"$PY_EXEC" -m pip install -q -r "$REQUIREMENTS"

# Запуск приложения как модуля (корректные импорты пакета app)
echo "Запуск Python-модуля $MAIN_MODULE..."
"$PY_EXEC" -m "$MAIN_MODULE"

# (Деактивация не требуется, так как окружение явно не активировалось)

# Задержка перед закрытием (10 секунд)
echo "Скрипт завершён. Окно закроется через 10 секунд..."
sleep 10
exit 0
