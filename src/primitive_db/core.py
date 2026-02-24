import os
import json
from .decorators import handle_db_errors, confirm_action, log_time

DB_FILE = "db.json"


def load_metadata(db_file=DB_FILE):
    """Загрузка метаданных из файла JSON."""
    if not os.path.exists(db_file):
        return {}
    with open(db_file, "r", encoding="utf-8") as f:
        return json.load(f)


def save_metadata(db_file, metadata):
    """Сохранение метаданных в файл JSON."""
    with open(db_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4, ensure_ascii=False)


def matches_where(record, where):
    """Проверка, соответствует ли запись условиям WHERE."""
    if not where:
        return True
    for key, value in where.items():
        if key not in record or str(record[key]) != str(value):
            return False
    return True


def parse_where(args):
    """Преобразует список условий в словарь."""
    where = {}
    for arg in args:
        if "=" in arg:
            key, value = arg.split("=", 1)
            where[key] = value
    return where


def parse_set(args):
    """Преобразует список SET аргументов в словарь."""
    set_dict = {}
    for arg in args:
        if "=" in arg:
            key, value = arg.split("=", 1)
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1]
            set_dict[key] = value
    return set_dict

def cast_value(value):
    """Пробуем привести значение к int, float, bool или оставляем str."""
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value

@handle_db_errors
def create_table(metadata, table_name, columns):
    """Создание таблицы с заданными колонками."""
    if table_name in metadata:
        print(f"Таблица '{table_name}' уже существует")
        return metadata

    metadata[table_name] = {"_columns": columns, "_records": []}
    print(f"Таблица '{table_name}' создана с колонками {list(columns.keys())}")
    save_metadata(DB_FILE, metadata)
    return metadata


@handle_db_errors
def list_tables(metadata):
    """Вывод списка таблиц и их колонок."""
    for table, info in metadata.items():
        cols = ", ".join(f"{k}:{v}" for k, v in info.get("_columns", {}).items())
        print(f"{table}: {cols}")


@handle_db_errors
@log_time
def insert(metadata, table_name, values):
    """Добавление записи в таблицу."""
    if table_name not in metadata:
        raise KeyError(table_name)

    columns = metadata[table_name].get("_columns")
    if not columns:
        raise KeyError("_columns")

    if len(values) != len(columns):
        raise ValueError("Количество значений не совпадает с количеством колонок.")

    record = dict(zip(columns.keys(), [cast_value(v) for v in values]))
    metadata[table_name]["_records"].append(record)
    save_metadata(DB_FILE, metadata)
    print(f"Запись добавлена: {record}")
    return metadata


@handle_db_errors
@log_time
def select_from(metadata, table_name, where=None):
    """Выборка записей из таблицы с учетом условий WHERE."""
    if table_name not in metadata:
        raise KeyError(table_name)

    records = metadata[table_name].get("_records", [])

    if where:
        records = [r for r in records if matches_where(r, where)]

    if not records:
        print(f"Таблица {table_name} пуста")
        return []

    for rec in records:
        print(rec)
    return records


@handle_db_errors
def update(metadata, table_name, where, updates):
    """Обновление записей по условию WHERE с автоприведением типов."""
    if table_name not in metadata:
        raise KeyError(table_name)

    updates = {k: cast_value(v) for k, v in updates.items()}

    records = metadata[table_name].get("_records", [])
    count = 0
    for rec in records:
        if matches_where(rec, where):
            rec.update(updates)
            count += 1

    print(f"Обновлено записей: {count}")
    save_metadata(DB_FILE, metadata)
    return metadata


@handle_db_errors
def delete(metadata, table_name, where):
    """Удаление записей по условию WHERE."""
    if table_name not in metadata:
        raise KeyError(table_name)

    records = metadata[table_name].get("_records", [])
    new_records = [r for r in records if not matches_where(r, where)]
    count = len(records) - len(new_records)
    metadata[table_name]["_records"] = new_records
    print(f"Удалено записей: {count}")
    save_metadata(DB_FILE, metadata)
    return metadata


@handle_db_errors
@confirm_action("удаление таблицы")
def drop_table(metadata, table_name):
    """Удаление всей таблицы."""
    if metadata is None:
        print("База данных пуста")
        return {}

    if table_name not in metadata:
        print(f"Таблица {table_name} не найдена")
        return metadata

    del metadata[table_name]
    save_metadata(DB_FILE, metadata)
    print(f"Таблица {table_name} удалена")
    return metadata
