from family_columns_to_rows import FamilyDataProcessor


if __name__ == "__main__":
    # 1 Семьи в строки
    # Укажите путь к вашему файлу
    processor = FamilyDataProcessor('excel_files/WINTARIZATION Total1.xlsx')
    processor.distribute_family_members()
    # 2 Находим дубли внутри таблицы
    # processor.mark_duplicates_with_details([11])
    processor.remove_duplicates([11])
    processor.save_result()
    # total_families_df = processor.get_result_df()

    # 3 Ищем совпадения в Генерале

    # 3.1 найденым проставляем статусы

    # 3.2 не найденым заводим новые UT


    # 4 Супики распределяют по хресникам
    # 5 Затягиваем в Генерал Хресников и обновляем им таблице

