import pandas as pd


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

    def compare_and_add_columns(self, comparison_df, base_columns, comparison_columns, output_file):
        """
        Сравнивает базовый DataFrame с переданным DataFrame и добавляет колонки на основе совпадений.

        Параметры:
            comparison_df (pd.DataFrame): DataFrame для сравнения.
            base_columns (list): Названия колонок в base_df для сравнения.
            comparison_columns (list): Индексы или названия колонок в comparison_df для сравнения.
            output_file (str): Имя выходного файла для сохранения результата.

        Возвращает:
            None: Результат сохраняется в указанный файл.
        """
        # Преобразуем индексы в названия колонок для comparison_df
        comparison_columns = [
            comparison_df.columns[i] if isinstance(i, int) else i for i in comparison_columns
        ]

        # Проверяем наличие указанных колонок в обоих DataFrame
        missing_columns_base = [col for col in base_columns if col not in self.base_df.columns]
        missing_columns_comparison = [
            col for col in comparison_columns if col not in comparison_df.columns
        ]

        if missing_columns_base:
            raise ValueError(f"Отсутствующие колонки в base_df: {missing_columns_base}")
        if missing_columns_comparison:
            raise ValueError(f"Отсутствующие колонки в comparison_df: {missing_columns_comparison}")

        # Выполняем слияние для поиска совпадений
        print("Начинаем поиск совпадений...")
        merged_df = self.base_df.merge(
            comparison_df[comparison_columns],  # Сужаем comparison_df до необходимых колонок
            left_on=base_columns,
            right_on=comparison_columns,
            how="left",
            indicator=True
        )

        # Добавляем новые столбцы
        merged_df["Анкета"] = merged_df["_merge"].apply(lambda x: "подано" if x == "both" else "")
        merged_df["Win2024"] = merged_df["_merge"].apply(lambda x: "на рассмотрении" if x == "both" else "")

        # Убираем служебный столбец "_merge"
        merged_df.drop(columns=["_merge"], inplace=True)

        # Сохраняем результат в указанный файл
        try:
            merged_df.to_excel(output_file, index=False)
            print(f"Результаты сохранены в файл '{output_file}'.")
        except Exception as e:
            raise ValueError(f"Ошибка при сохранении файла '{output_file}': {e}")

    def get_base_df(self):
        return self.base_df


class DataFrameProcessor:
    def __init__(self, base_df, comparison_df):
        self.base_df = base_df
        self.comparison_df = comparison_df

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
                print(f"base_col:{base_col}", f"comp_col:{comp_col_int}")
                print(unmatched_rows.columns)
                print(f"Checking comp_col: {comp_col_int} ({type(comp_col_int)}) in columns: {list(unmatched_rows.columns)}")
                if comp_col_int in unmatched_rows.columns:

                    print(f"added comp_col:{comp_col_int}")
                    new_row[base_col] = row[comp_col_int]
                else:
                    print(f"not found comp_col:{comp_col_int} in unmatched_rows:{unmatched_rows}")
                    new_row[base_col] = ""  # Если колонка отсутствует в comparison_df
            new_rows = pd.concat([new_rows, pd.DataFrame([new_row])], ignore_index=True)

        # Сохраняем в файл
        new_rows.to_excel(output_file, index=False)
        print(f"Обработанные строки сохранены в файл: {output_file}")

    @staticmethod
    def map_columns(base_df, comparison_df):
        """
        Выводит сопоставление колонок base_df и comparison_df для настройки.
        """
        print("Колонки из base_df:")
        print(list(base_df.columns))
        print("\nКолонки из comparison_df:")
        print(list(comparison_df.columns))


