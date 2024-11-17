from family_columns_to_rows import FamilyDataProcessor
# 0 Открівваем файл

# Пример использования
if __name__ == "__main__":
    # Укажите путь к вашему файлу
    processor = FamilyDataProcessor('WINTARIZATION Total1.xlsx')
    processor.distribute_family_members()

# 1 Семьи в строки
# 2 Находим дубли внутри таблицы

# 3 Ищем совпадения в Генерале
# 3.1 найденым проставляем статусы
# 3.2 не найденым заводим новые UT

# 4 Супики распределяют по хресникам
# 5 Затягиваем в Генерал Хресников и обновляем им таблице

