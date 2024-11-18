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


