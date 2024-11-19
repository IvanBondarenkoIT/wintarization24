import pandas as pd

from family_files_manager import FamilyDataProcessor
from general_file_manager import ExcelDataMatcher
from general_file_manager import DataFrameProcessor


RUN_WIN24_FAMILY_PROCESSOR = False
RUN_UT_SEEKER = True
RUN_NEW_ROW_CREATOR = False


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

    # Загрузка данных
    if RUN_WIN24_FAMILY_PROCESSOR:
        processor = FamilyDataProcessor('excel_files/WINTARIZATION Total1.xlsx')
        processor.distribute_family_members()
        # 2 Находим дубли внутри таблицы
        # processor.mark_duplicates_with_details([11])
        processor.remove_duplicates(LIST_OF_COLUMNS_TO_CHECK_DUPLICATES)
        processor.save_result()
        total_families_df = processor.get_result_df()

    if RUN_UT_SEEKER:
        total_families_df = pd.read_excel("excel_files/processed_family_data (2).xlsx")
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


    if RUN_NEW_ROW_CREATOR:
        total_families_df = pd.read_excel("excel_files/processed_family_data(2).xlsx")
        base_df = pd.read_excel(EXCEL_GENERAL_FILE_PATH, sheet_name=GENERAL_FILE_SHEET_NAME)
        # 3.2 не найденым заводим новые UT
        new_rows_creator = DataFrameProcessor(base_df, total_families_df)
        # Настраиваем сопоставление колонок
        new_rows_creator.map_columns(base_df, total_families_df)
        column_mapping = {
            "phone ukr": "2",
            "georgian phone": "3",
            "CityinGeorgia": "4",
            "Adress in Georgia": "5",
            "Surname": "7",
            "Name": "8",
            "Gender": "9",
            "Document type": "10",
            "Numberdoc": "11",

            "Date of birth": "12",
            "Date of arrival": "13",
            "Citizenship": "14",

            "bank": "17",
            "iban": "18",

            # Добавьте остальные пары
        }

        # Сравнение данных
        base_columns = GENERAL_COLUMNS
        comparison_columns = FAMILIES_COLUMNS
        unmatched_rows = new_rows_creator.compare_and_add_columns(base_columns, comparison_columns)

        # # Обрабатываем несовпадения
        new_rows_creator.handle_unmatched_rows(unmatched_rows, column_mapping, "new_unmatched_rows.xlsx")


    # 4 Супики распределяют по хресникам
    # 5 Затягиваем в Генерал Хресников и обновляем им таблице

