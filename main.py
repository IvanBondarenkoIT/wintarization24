from family_files_manager import FamilyDataProcessor
from general_file_manager import ExcelDataMatcher

LIST_OF_COLUMNS_TO_CHECK_DUPLICATES = [11]
EXCEL_FAMILIES_FILE_PATH = "excel_files/WINTARIZATION Total1.xlsx"
EXCEL_GENERAL_FILE_PATH = "excel_files/General base.xlsx"
GENERAL_FILE_SHEET_NAME = "ua"

# Сравниваемые колонки
GENERAL_COLUMNS = ["Numberdoc"]  # Названия колонок из base_df
FAMILIES_COLUMNS = [11]  # Индексы или названия колонок из comparison_df


if __name__ == "__main__":
    # 1 Семьи в строки
    # Укажите путь к вашему файлу

    processor = FamilyDataProcessor('excel_files/WINTARIZATION Total1.xlsx')
    processor.distribute_family_members()
    # 2 Находим дубли внутри таблицы
    # processor.mark_duplicates_with_details([11])
    processor.remove_duplicates(LIST_OF_COLUMNS_TO_CHECK_DUPLICATES)
    processor.save_result()
    total_families_df = processor.get_result_df()

    # 3 Ищем совпадения в Генерале
    matcher = ExcelDataMatcher(base_file=EXCEL_GENERAL_FILE_PATH, sheet_name=GENERAL_FILE_SHEET_NAME)
    # 3.1 найденым проставляем статусы
    # Выполнение сравнения и сохранение результата
    matcher.compare_and_add_columns(
        comparison_df=total_families_df,
        base_columns=GENERAL_COLUMNS,
        comparison_columns=FAMILIES_COLUMNS,
        output_file="processed_general_base.xlsx"
    )


    # 3.2 не найденым заводим новые UT


    # 4 Супики распределяют по хресникам
    # 5 Затягиваем в Генерал Хресников и обновляем им таблице

