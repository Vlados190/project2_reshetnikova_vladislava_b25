import time
from functools import wraps


def handle_db_errors(func):
    """Декоратор для обработки ошибок базы данных."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            print(
                "Ошибка: Файл данных не найден. "
                "Возможно, база данных не инициализирована."
            )
        except KeyError as e:
            print(f"Ошибка: Таблица или столбец {e} не найден.")
        except ValueError as e:
            print(f"Ошибка валидации: {e}")
        except Exception as e:
            print(f"Произошла непредвиденная ошибка: {e}")
    return wrapper


def confirm_action(action_name):
    """Декоратор для подтверждения действия пользователем."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            answer = input(
                f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: '
            )
            if answer.lower() != "y":
                print("Операция отменена")
                return args[0]  # возвращаем метаданные без изменений
            return func(*args, **kwargs)
        return wrapper
    return decorator


def log_time(func):
    """Декоратор для логирования времени выполнения функции."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        result = func(*args, **kwargs)
        end = time.monotonic()
        print(f"Функция {func.__name__} выполнилась за {end - start:.3f} секунд")
        return result
    return wrapper


def cache_result():
    """Декоратор кэша для функций."""
    cache = {}

    def decorator(func):
        @wraps(func)
        def wrapper(*args):
            key = tuple(args)
            if key in cache:
                return cache[key]
            result = func(*args)
            cache[key] = result
            return result
        return wrapper
    return decorator