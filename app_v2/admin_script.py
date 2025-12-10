import sys
import os
import asyncio
from app_v2.config import config as app_config
from app_v2.uploader.uploader import Uploader


def print_menu():
    print("\nАдмин-утилита GrandMedExtractorV2")
    print("1. Скачать все требуемые файлы")
    print("0. Выход")


def check_env():
    # Проверка Python версии
    min_py = (3, 8)
    if sys.version_info < min_py:
        print(f"[ОШИБКА] Необходим Python >= {min_py[0]}.{min_py[1]}, а у вас {sys.version}")
        sys.exit(1)
    # Проверка, что активирована venv
    if not hasattr(sys, 'real_prefix') and sys.prefix == sys.base_prefix:
        print("[ОШИБКА] Виртуальное окружение не активировано!")
        print("Пожалуйста, активируйте venv через 'source venv/bin/activate' или 'venv\\Scripts\\activate'.")
        sys.exit(1)
    print("Pipeline environment is OK.")


def main():
    while True:
        print_menu()
        choice = input("Выберите действие: ").strip()
        if choice == "1":
            check_env()  # Вставили здесь
            run_main_pipeline()
        elif choice == "0":
            print("Выход...")
            sys.exit(0)
        else:
            print("Некорректный выбор. Повторите.")

def run_main_pipeline():
    config = {
        "browser": {"chromium_path": "browsers/chromium/chrome-win/chrome.exe", "headless": False},
        **app_config.actions,
        **app_config.postgres,
        **app_config.bitrix,
        **app_config.telegram,
        "site": app_config.actions["site"] if "site" in app_config.actions else {"url": "https://example.com"},
    }
    uploader = Uploader(config)
    uploader.run()

if __name__ == "__main__":
    main()
