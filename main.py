import pandas as pd

from family_files_manager import FamilyDataProcessor
from general_file_manager import ExcelDataMatcher
from general_file_manager import DataFrameProcessor
from general_update_module import MultiSheetExcelMatcher
from searcher_by_date_and_phone import GeneralMatcherDateAndPhone

RUN_WIN24_FAMILY_PROCESSOR = False # Обрабатываем таблицу с поданными формами на регистрацию в WIN24
RUN_UT_SEEKER = False # Ищем совпадения в Генерале в processed_family_data нужно переименовать колонку "11" в "id", а в генерале колонка "Numberdoc" на 'id'
RUN_DATE_AND_PHONE_MATCHER = True # Ищем совпадения в Генерале по дате рождения и номеру телефона колонку "3"-> georgian_phone и колонку "12"-> birth_date
RUN_NEW_ROW_CREATOR = False # Создаем новые строки для переноса в Генерал

RUN_BILLS_CHECKER_UPDATER = False # Обновляем статусы в Генерал по результатам проверки IBAN


LIST_OF_COLUMNS_TO_CHECK_DUPLICATES = [11]
EXCEL_FAMILIES_FILE_PATH = "excel_files/WINTARIZATION_2024_3_06.xlsx"  # Путь к файлу с формами (добавить три колонки с банковскими данными)
# EXCEL_FAMILIES_FILE_PATH = "excel_files/CASH3_Questionnaire of Ukrainians_ (Ответы).xlsx"
EXCEL_GENERAL_FILE_PATH = "excel_files/General base (11).xlsx"  # Путь к файлу General последней версии
EXCEL_WITH_RESULTS_OF_BILL_CHECKER_PATH = "excel_files/all bills checked.xlsx"  # Путь к файлу в котором проверенные IBAN от хресников
GENERAL_FILE_SHEET_NAME = "ua"

# Сравниваемые колонки
GENERAL_COLUMNS = ["Numberdoc"]  # Названия колонок из base_df
# GENERAL_COLUMNS = ["id"]  # Названия колонок из base_df

FAMILIES_COLUMNS = [1]  # Индексы или названия колонок из comparison_df
# FAMILIES_COLUMNS = [0]  # Индексы или названия колонок из comparison_df


if __name__ == "__main__":
    # 1 Семьи в строки
    # Укажите путь к вашему файлу

    # Загрузка данных
    if RUN_WIN24_FAMILY_PROCESSOR:
        # processor = FamilyDataProcessor('excel_files/WINTARIZATION Total1.xlsx')
        # processor = FamilyDataProcessor('excel_files/WINTARIZATION_2024_3_06.xlsx')
        processor = FamilyDataProcessor(EXCEL_FAMILIES_FILE_PATH)
        # processor = FamilyDataProcessor('excel_files/CASH3_Questionnaire of Ukrainians_ (Ответы).xlsx')
        processor.distribute_family_members()
        # 2 Находим дубли внутри таблицы
        # processor.mark_duplicates_with_details([11])
        # processor.remove_duplicates(LIST_OF_COLUMNS_TO_CHECK_DUPLICATES)
        processor.save_result()
        total_families_df = processor.get_result_df()

    if RUN_UT_SEEKER:
        # total_families_df = pd.read_excel("excel_files/UT которым нужен статус.xlsx")
        total_families_df = pd.read_excel("excel_files/processed_family_data.xlsx")
        # 3 Ищем совпадения в Генерале
        matcher = ExcelDataMatcher(base_file=EXCEL_GENERAL_FILE_PATH, sheet_name=GENERAL_FILE_SHEET_NAME)
        # 3.1 найденым проставляем статусы
        # Выполнение сравнения и сохранение результата
        matcher.compare_and_update_statuses(
            comparison_df=total_families_df,
            # base_columns=GENERAL_COLUMNS,
            # comparison_columns=FAMILIES_COLUMNS,
            output_file="processed_general_base.xlsx",
            unmatched_file="unmatched_general_base.xlsx"
        )

    if RUN_DATE_AND_PHONE_MATCHER:
        # Использование класса
        general_file = EXCEL_GENERAL_FILE_PATH
        unmatched_file = "excel_files/unmatched_general_base.xlsx"
        matched_output_file = "Matched_Rows.xlsx"
        unmatched_output_file = "Unmatched_Rows.xlsx"
        # +++++++++++++++
        matched_phone_file = "matched_by_phone.xlsx"
        unmatched_phone_file = "unmatched_by_phone.xlsx"
        matched_date_file = "matched_by_date.xlsx"
        unmatched_date_file = "unmatched_by_date.xlsx"
        final_unmatched_date_file = "final_unmatched_by_date.xlsx"

        # Создание объекта класса
        matcher = GeneralMatcherDateAndPhone(general_file=general_file, unmatched_file=unmatched_file)

        # Вызов метода с четырьмя файлами
        matcher.match_and_save(matched_phone_file, unmatched_phone_file, matched_date_file, unmatched_date_file, final_unmatched_date_file)
        # ---------------
        #
        # matcher = GeneralMatcherDateAndPhone(general_file, unmatched_file)
        # matcher.match_and_save(matched_output_file, unmatched_output_file)

    if RUN_NEW_ROW_CREATOR:
        # total_families_df = pd.read_excel("excel_files/processed_family_data (2).xlsx")
        total_families_df = pd.read_excel("excel_files/processed_family_data.xlsx")
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
            "R ind 12": "15",
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

    if RUN_BILLS_CHECKER_UPDATER:
        matcher = MultiSheetExcelMatcher(EXCEL_GENERAL_FILE_PATH, EXCEL_WITH_RESULTS_OF_BILL_CHECKER_PATH)
        matcher.update_status("Updated-General.xlsx")

    # 4 Супики распределяют по хресникам
    # 5 Затягиваем в Генерал Хресников и обновляем им таблице

