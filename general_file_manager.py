import pandas as pd
import re  # Для работы с регулярными выражениями
import logging


NEW_UT_COLUMN_MAPPING = {
            4: {  # Например, колонка "CityinGeorgia"
                "Тбілісі": "Tbilisi",
                "Руставі": "Rustavi",
                "Батумі": "Batumi",
                "Кутаїсі": "Kutaisi",
                "Кобулеті": "Kobuleti",
                "Поті": "Poti",
                "Горі": "Gori",
                "Чакві": "Chakvi",
                "Зугдіді": "Zugdid",
            },
            9: {  # Например, колонка "Gender"
                "Чоловік": "Male",
                "Жінка": "Female"
            },

            10: {  # Например, колонка "Document"
                "Закордонний Паспорт": "International Passport",
                "Український внутрішній паспорт": "Internal Passport",
                "Свидетельство о рождении": "Birth Certificate"
            },
            14: {  # Например, колонка "Citizenship"
                "Українець": "Ukrainian",
                "Грузин": "Georgian",
            },
            15: {  # "Чи відноситесь Ви до однієї з груп вразливості?"
                "Родина з дитиною від 0 до 5 років включно": "K",
                "Так, з інвалідністю чи обмеженими можливостями або тяжко хворий": "A",
                "Так, старше 60 років": "B",
                "Так, багатодітна родина": "C",
                "Так, одинока мати/ бактько, що самостійно виховує неповнолітніх дітей": "D",
                "Ні": "E",
            },

        }

# {
#             "phone ukr": "2",
#             "georgian phone": "3",
#             "CityinGeorgia": "4",
#             "Adress in Georgia": "5",
#             "Surname": "7",
#             "Name": "8",
#             "Gender": "9",
#             "Document type": "10",
#             "Numberdoc": "11",
#
#             "Date of birth": "12",
#             "Date of arrival": "13",
#             "Citizenship": "14",
#
#             "bank": "17",
#             "iban": "18",
#
#             # Добавьте остальные пары
#         }

class ExcelDataMatcher:
    """
    Класс для сравнения и обработки данных между двумя Excel файлами.

    Атрибуты:
        base_df (pd.DataFrame): Основной DataFrame для сравнения.
    """

    def __init__(self, base_file, sheet_name):
        """
        Инициализация класса ExcelDataMatcher.

        Параметры:
            base_file (str): Путь к Excel файлу для загрузки базовых данных.
            sheet_name (str): Название вкладки для загрузки.
        """
        try:
            self.base_df = pd.read_excel(base_file, sheet_name=sheet_name)
            print(f"Загружена вкладка '{sheet_name}' из файла '{base_file}'.")
        except Exception as e:
            raise ValueError(f"Ошибка при загрузке файла '{base_file}': {e}")

    # Настраиваем логирование
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def compare_and_update_statuses(self, comparison_df, output_file, unmatched_file):
        """
        Обновляет статусы в базовом DataFrame на основе совпадений с comparison_df.
        Создает новую колонку "Новая Анкета" с статусом "подано" для совпадающих ID.
        Сохраняет значения ненайденных ID в отдельный файл (unmatched_file).
        """
        # Приведение базового DataFrame к строковому типу и обработка пустых значений
        base_df_copy = self.base_df.copy()

        # Преобразуем все ID в числовые значения (оставляем только цифры)
        base_df_copy["id_clean"] = base_df_copy["id"].str.extract(r'(\d+)').fillna("").astype(str)
        comparison_df_copy = comparison_df.copy()
        comparison_df_copy["id_clean"] = comparison_df_copy["id"].str.extract(r'(\d+)').fillna("").astype(str)

        # Получаем список ID для обновления
        ids_to_update = set(comparison_df_copy["id_clean"])

        # Создаем DataFrame для строк, у которых нет совпадений
        unmatched_df = comparison_df_copy[~comparison_df_copy["id_clean"].isin(base_df_copy["id_clean"])]

        # Логирование процесса сравнения
        print("Процесс сравнения ID:")
        for comparison_id in comparison_df_copy["id_clean"]:
            base_match = comparison_id in base_df_copy["id_clean"].values
            print(f"Сравниваем ID: {comparison_id} -> Совпадение: {base_match}")

        # Создаем новую колонку "Новая Анкета" и проставляем "подано" для совпадающих ID
        base_df_copy["Новая Анкета"] = base_df_copy["Анкета"]  # Копируем старые значения
        base_df_copy.loc[base_df_copy["id_clean"].isin(ids_to_update), "Новая Анкета"] = "подано"

        # Убираем временный столбец id_clean
        base_df_copy.drop(columns=["id_clean"], inplace=True)

        # Сохраняем базовый DataFrame с обновленными статусами
        try:
            base_df_copy.to_excel(output_file, index=False)
            print(f"Результаты сохранены в файл '{output_file}'.")
        except Exception as e:
            raise ValueError(f"Ошибка при сохранении файла '{output_file}': {e}")

        # Сохраняем unmatched_df
        try:
            unmatched_df.drop(columns=["id_clean"], inplace=True)  # Убираем вспомогательный столбец
            unmatched_df.to_excel(unmatched_file, index=False)
            print(f"Несовпавшие ID сохранены в файл '{unmatched_file}'.")
        except Exception as e:
            raise ValueError(f"Ошибка при сохранении файла '{unmatched_file}': {e}")

        self.synchronize_statuses(output_file)

    def synchronize_statuses(self, output_file):
        """
        Пробегает по результирующему файлу и для всех строк с одинаковым значением в колонке 'ut',
        где в колонке 'Новая Анкета' стоит статус 'подано', проставляет 'подано' во всех строках с таким же 'ut'.
        """
        # Загружаем DataFrame из файла
        base_df_copy = pd.read_excel(output_file)

        # Находим все строки, у которых в "Новая Анкета" статус "подано"
        ut_with_submitted = base_df_copy[base_df_copy["Новая Анкета"] == "подано"]["ut"].unique()

        # Обновляем статус "Новая Анкета" для всех строк с совпадающим ut
        base_df_copy.loc[base_df_copy["ut"].isin(ut_with_submitted), "Новая Анкета"] = "подано"

        # Сохраняем обновленный DataFrame обратно в файл
        try:
            base_df_copy.to_excel(output_file, index=False)
            print(f"Статусы обновлены и результаты сохранены в файл '{output_file}'.")
        except Exception as e:
            raise ValueError(f"Ошибка при сохранении файла '{output_file}': {e}")

    # def synchronize_statuses(self, df, id_column, status_columns):
    #     """
    #     Синхронизирует статусы для всех строк с одинаковым значением id.
    #     Статусы обновляются только для строк, где они изначально пустые.
    #     Группы, где все строки пустые, остаются без изменений.
    #
    #     Параметры:
    #         df (pd.DataFrame): DataFrame для обработки.
    #         id_column (str): Название колонки, содержащей идентификаторы (например, "id").
    #         status_columns (list): Список колонок со статусами для синхронизации (например, ["Анкета", "Win2024"]).
    #
    #     Возвращает:
    #         pd.DataFrame: Обновлённый DataFrame с синхронизированными статусами.
    #     """
    #     print("Синхронизация статусов...")
    #
    #     # Преобразуем пустые строки в NaN для корректной обработки
    #     df[status_columns] = df[status_columns].replace("", None)
    #
    #     for col in status_columns:
    #         # Найдём максимальный статус для каждой группы id
    #         group_status = df.groupby(id_column)[col].transform(lambda x: x.bfill().ffill() if x.notna().any() else x)
    #
    #         # Только пустые значения получают статус из группы
    #         df[col] = df[col].where(df[col].notna(), group_status)
    #
    #     print("Статусы успешно синхронизированы.")
    #     return df

    def get_base_df(self):
        return self.base_df


class DataFrameProcessor:
    def __init__(self, base_df, comparison_df):
        self.base_df = base_df
        self.comparison_df = comparison_df

        self.column_mappings = NEW_UT_COLUMN_MAPPING

    def compare_and_add_columns(self, base_columns, comparison_columns):
        """
        Сравнивает строки между base_df и comparison_df по указанным колонкам.
        Возвращает строки из comparison_df, которые не нашли совпадений в base_df.
        """
        base_subset = self.base_df[base_columns].astype(str)
        comparison_subset = self.comparison_df[comparison_columns].astype(str)

        unmatched_mask = ~comparison_subset.apply(tuple, axis=1).isin(base_subset.apply(tuple, axis=1))
        unmatched_rows = self.comparison_df[unmatched_mask]
        print(f"Количество несовпадающих строк: {unmatched_rows.shape[0]}{unmatched_rows}")
        return unmatched_rows

    def handle_unmatched_rows(self, unmatched_rows, column_mapping, output_file):
        """
        Обрабатывает строки из unmatched_rows, создаёт новый DataFrame в формате base_df.
        Сохраняет результат в указанный файл.
        """
        # Создаём DataFrame с той же структурой, что и base_df
        new_rows = pd.DataFrame(columns=self.base_df.columns)

        for _, row in unmatched_rows.iterrows():
            new_row = {}

            for base_col, comp_col in column_mapping.items():
                comp_col_int = int(comp_col)  # Приводим comp_col к числу
                # print(f"base_col:{base_col}", f"comp_col:{comp_col_int}")
                # print(unmatched_rows.columns)
                # print(f"Checking comp_col: {comp_col_int} ({type(comp_col_int)}) in columns: {list(unmatched_rows.columns)}")
                if comp_col_int in unmatched_rows.columns:

                    # print(f"added comp_col:{comp_col_int}")
                    new_row[base_col] = self.rebuild_value_needed_format(column=comp_col_int, value=row[comp_col_int])
                else:
                    # print(f"not found comp_col:{comp_col_int} in unmatched_rows:{unmatched_rows}")
                    new_row[base_col] = ""  # Если колонка отсутствует в comparison_df

            new_row["Анкета"] = "подано"
            new_row["Win2024"] = "на рассмотрении"

            new_rows = pd.concat([new_rows, pd.DataFrame([new_row])], ignore_index=True)

        # Сохраняем в файл
        new_rows.to_excel(output_file, index=False)
        print(f"Обработанные строки сохранены в файл: {output_file}")

    def rebuild_value_needed_format(self, column, value):
        """
        Проверяет значение и возвращает соответствующее значение из маппинга.
        Если соответствие не найдено, пытается найти частичное совпадение.
        """
        print(f"column: {column}, value: {value}")
        # Получаем маппинг для конкретной колонки
        mappings = self.column_mappings.get(column, {})

        # Приводим значение к единому формату
        cleaned_value = str(value).strip().lower()

        # Пробуем найти точное совпадение
        normalized_mappings = {str(k).strip().lower(): v for k, v in mappings.items()}
        if cleaned_value in normalized_mappings:
            return normalized_mappings[cleaned_value]

        # Если точного совпадения нет, ищем частичное совпадение
        for key, mapped_value in normalized_mappings.items():
            if key in cleaned_value:  # Проверяем, содержится ли часть строки
                return mapped_value

        # Если ничего не найдено, возвращаем оригинальное значение
        return value


    @staticmethod
    def map_columns(base_df, comparison_df):
        """
        Выводит сопоставление колонок base_df и comparison_df для настройки.
        """
        print("Колонки из base_df:")
        print(list(base_df.columns))
        print("\nКолонки из comparison_df:")
        print(list(comparison_df.columns))


