import shlex
from .core import (
    load_metadata,
    save_metadata,
    create_table,
    drop_table,
    list_tables,
    insert,
    select_from,
    update,
    delete,
    parse_where,
    parse_set,
)

DB_FILE = "db.json"


def print_help():
    """Вывод справки по командам."""
    print("\n*** Процесс работы с таблицей ***")
    print("Функции:")
    print(
        "<command> create_table <имя_таблицы> <столбец1:тип> .. "
        "- создать таблицу"
    )
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print("<command> insert into <таблица> values (...) - добавить запись")
    print(
        "<command> select from <таблица> [where <условие>] "
        "- вывести записи"
    )
    print(
        "<command> update <таблица> set <столбец=значение> "
        "where <условие> - изменить запись"
    )
    print("<command> delete from <таблица> where <условие> - удалить запись")
    print("<command> info <таблица> - информация о таблице")
    print("<command> exit - выход")
    print("<command> help - справка\n")


def run():
    """Основной цикл работы с базой данных."""
    while True:
        metadata = load_metadata(DB_FILE)
        user_input = input(">>>Введите команду: ")
        try:
            args = shlex.split(user_input)
        except ValueError:
            print("Ошибка парсинга команды.")
            continue

        if not args:
            continue

        cmd = args[0].lower()

        if cmd == "help":
            print_help()
        elif cmd == "exit":
            break

        # CREATE TABLE
        elif cmd == "create_table":
            if len(args) < 3:
                print("Использование: create_table <имя> <столбцы>")
                continue
            table_name = args[1]
            columns_list = args[2:]
            columns = {}
            for col in columns_list:
                if ":" not in col:
                    print(f"Некорректный формат столбца: {col}")
                    continue
                name, typ = col.split(":", 1)
                columns[name] = typ
            metadata = create_table(metadata, table_name, columns)
            save_metadata(DB_FILE, metadata)

        # LIST TABLES
        elif cmd == "list_tables":
            list_tables(metadata)

        # DROP TABLE
        elif cmd == "drop_table":
            if len(args) < 2:
                print("Укажите имя таблицы для удаления.")
                continue
            table_name = args[1]
            metadata = drop_table(metadata, table_name)
            save_metadata(DB_FILE, metadata)

        # INSERT
        elif cmd == "insert":
            if len(args) < 4 or args[1].lower() != "into":
                print("Используйте insert into <таблица> values (...)")
                continue
            table_name = args[2]
            values_str = user_input.split("values", 1)[1].strip()
            if values_str.startswith("(") and values_str.endswith(")"):
                values_str = values_str[1:-1]
                values = [
                    v.strip().strip('"').strip("'") for v in values_str.split(",")
                ]
                metadata = insert(metadata, table_name, values)
            else:
                print(
                    "Некорректная команда insert. Используйте insert into "
                    "<таблица> values (...)"
                )

        # SELECT
        elif cmd == "select":
            if len(args) < 3 or args[1].lower() != "from":
                print("Используйте select from <таблица> [where <условие>]")
                continue
            table_name = args[2]
            where_clause = None
            if "where" in args:
                where_index = args.index("where")
                where_clause = parse_where(args[where_index + 1:])
            select_from(metadata, table_name, where_clause)

        # UPDATE
        elif cmd == "update":
            if "set" not in args or "where" not in args:
                print("Используйте update <таблица> set ... where ...")
                continue
            table_name = args[1]
            set_index = args.index("set")
            where_index = args.index("where")
            set_clause = parse_set(args[set_index + 1:where_index])
            where_clause = parse_where(args[where_index + 1:])
            metadata = update(metadata, table_name, where_clause, set_clause)
            save_metadata(DB_FILE, metadata)

        # DELETE
        elif cmd == "delete":
            if len(args) < 4 or args[1].lower() != "from" or "where" not in args:
                print("Используйте delete from <таблица> where ...")
                continue
            table_name = args[2]
            where_index = args.index("where")
            where_clause = parse_where(args[where_index + 1:])
            metadata = delete(metadata, table_name, where_clause)
            save_metadata(DB_FILE, metadata)

        # INFO
        elif cmd == "info":
            if len(args) < 2:
                print("Укажите имя таблицы для информации.")
                continue
            table_name = args[1]
            if table_name in metadata:
                print(f"Таблица: {table_name}")
                columns = metadata[table_name].get("_columns", {})
                print(
                    "Столбцы: "
                    + ", ".join([f"{k}:{v}" for k, v in columns.items()])
                )
                records = metadata[table_name].get("_records", [])
                print(f"Количество записей: {len(records)}")
            else:
                print(f'Таблица "{table_name}" не существует.')
        else:
            print(f"Функции {cmd} нет. Попробуйте снова.")