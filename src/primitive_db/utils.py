import os
import json

DATA_DIR = "data"


def load_metadata(file_name):
    """Загрузка метаданных из файла."""
    if not os.path.exists(file_name):
        return {}
    with open(file_name, "r", encoding="utf-8") as f:
        return json.load(f)


def save_metadata(file_name, data):
    """Сохранение метаданных в файл."""
    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def load_table_data(table_name):
    """Загрузка данных конкретной таблицы из JSON."""
    path = os.path.join(DATA_DIR, f"{table_name}.json")
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_table_data(table_name, data):
    """Сохранение данных конкретной таблицы в JSON."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    path = os.path.join(DATA_DIR, f"{table_name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)