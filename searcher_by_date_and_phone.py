import pandas as pd

class GeneralMatcherDateAndPhone:
    """
    Класс для сравнения данных между General и Unmatched General Base.
    """

    def __init__(self, general_file, unmatched_file):
        """
        Инициализация класса GeneralMatcher.

        Параметры:
            general_file (str): Путь к файлу General.
            unmatched_file (str): Путь к файлу Unmatched General Base.
        """
        try:
            # Загружаем General и выбираем только нужные колонки
            self.general_df = pd.read_excel(
                general_file,
                sheet_name="ua",
                usecols=["Date of birth", "georgian phone", "ut", "Surname", "Name", "id"]
            )
            print(f"Файл General (вкладка ua) успешно загружен: {general_file}.")
        except Exception as e:
            raise ValueError(f"Ошибка при загрузке General: {e}")

        try:
            # Загружаем Unmatched General Base
            self.unmatched_df = pd.read_excel(unmatched_file)
            print(f"Файл Unmatched General Base успешно загружен: {unmatched_file}.")
        except Exception as e:
            raise ValueError(f"Ошибка при загрузке Unmatched General Base: {e}")

        # Обработка данных
        self._clean_data()

    def _clean_data(self):
        """
        Приведение данных к единому формату.
        """
        # Приведение даты рождения к стандартному формату
        self.general_df["Date of birth"] = pd.to_datetime(
            self.general_df["Date of birth"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

        self.unmatched_df["birth_date"] = pd.to_datetime(
            self.unmatched_df["birth_date"], format="%d.%m.%Y", dayfirst=True, errors="coerce"
        ).dt.strftime("%Y-%m-%d")

        # Оставляем только последние 6 цифр в номере телефона
        self.general_df["georgian_phone_last6"] = self.general_df["georgian phone"].astype(str).str[-6:]
        self.unmatched_df["georgian_phone_last6"] = self.unmatched_df["georgian_phone"].astype(str).str[-6:]

    def match_by_phone(self):
        """
        Первый этап: поиск совпадений по номеру телефона.
        Если найдено совпадение, переносим всю семью (по совпадающему номеру телефона).
        """
        matched_rows = []
        unmatched_rows = []

        # Итерация по строкам из unmatched_df
        for _, row in self.unmatched_df.iterrows():
            unmatched_phone_last6 = row["georgian_phone_last6"]

            # Поиск совпадений по последним 6 цифрам телефона
            family_matches = self.general_df[self.general_df["georgian_phone_last6"] == unmatched_phone_last6]

            if not family_matches.empty:
                # Переносим всю семью (все строки с этим номером телефона)
                for _, family_member in family_matches.iterrows():
                    matched_row = row.copy()
                    matched_row["ut"] = family_member["ut"]
                    matched_row["Surname"] = family_member["Surname"]
                    matched_row["Name"] = family_member["Name"]
                    matched_row["Matched By"] = f"Phone: {unmatched_phone_last6}"
                    matched_row["georgian_phone"] = family_member["georgian phone"]  # Добавляем телефон
                    matched_rows.append(matched_row)

                # Удаляем строки с таким же номером телефона из unmatched_df
                self.unmatched_df = self.unmatched_df[self.unmatched_df["georgian_phone_last6"] != unmatched_phone_last6]
            else:
                unmatched_rows.append(row)

        return pd.DataFrame(matched_rows), pd.DataFrame(unmatched_rows)

    def match_by_date_of_birth(self, unmatched_df):
        """
        Второй этап: поиск совпадений по дате рождения.
        Если найдено совпадение, переносим всю семью (по номеру телефона совпавшего)
        в таблицу с совпадениями.
        """
        matched_rows = []
        remaining_rows = unmatched_df.copy()  # Копируем для дальнейшей обработки

        for _, row in unmatched_df.iterrows():
            unmatched_birth_date = row["birth_date"]

            # Поиск совпадений по дате рождения
            matches = self.general_df[self.general_df["Date of birth"] == unmatched_birth_date]

            if not matches.empty:
                # Для каждого найденного совпадения добавляем всю семью
                for _, match in matches.iterrows():
                    family_phone = match["georgian phone"]

                    # Приводим номер телефона к строке для индексации
                    family_phone_str = str(family_phone)

                    # Поиск всех членов семьи с тем же номером телефона
                    family_matches = self.general_df[self.general_df["georgian phone"] == family_phone]

                    for _, family_member in family_matches.iterrows():
                        matched_row = row.copy()
                        matched_row["ut"] = family_member["ut"]
                        matched_row["Surname"] = family_member["Surname"]
                        matched_row["Name"] = family_member["Name"]
                        matched_row["Matched By"] = f"Date of birth: {unmatched_birth_date}, Phone: {family_phone_str[-6:]}"
                        matched_row["georgian_phone"] = family_member["georgian phone"]  # Добавляем телефон
                        matched_rows.append(matched_row)

                    # Удаляем все строки с таким же номером телефона из оставшихся строк
                    family_phone_mask = unmatched_df["georgian_phone_last6"] == str(family_phone)[-6:]
                    remaining_rows = remaining_rows[~family_phone_mask]

        matched_df = pd.DataFrame(matched_rows)
        return matched_df, remaining_rows

    def clean_unmatched_by_phone(self, matched_df):
        """
        Удаляет из unmatched_df строки, которые имеют совпадающий номер телефона с теми, что в matched_df.
        """
        matched_phones = matched_df["georgian_phone"].unique()  # Получаем уникальные телефоны из совпавших

        # Удаляем строки из unmatched_df, которые имеют совпадающие телефоны
        self.unmatched_df = self.unmatched_df[~self.unmatched_df["georgian_phone"].isin(matched_phones)]

    def match_and_save(self, matched_file, unmatched_file):
        """
        Выполняет оба этапа поиска и сохраняет результаты в файлы.

        Параметры:
            matched_file (str): Путь для сохранения совпадающих строк.
            unmatched_file (str): Путь для сохранения несовпадающих строк.
        """
        # Первый этап: поиск по номеру телефона
        matched_by_phone, unmatched_after_phone = self.match_by_phone()

        # Второй этап: поиск по дате рождения
        matched_by_birth, unmatched_after_birth = self.match_by_date_of_birth(unmatched_after_phone)

        # Объединяем все совпадения
        matched_df = pd.concat([matched_by_phone, matched_by_birth], ignore_index=True)

        # Чистим unmatched_df от строк с совпадающими номерами телефонов
        self.clean_unmatched_by_phone(matched_df)

        # Объединяем все оставшиеся несовпадения
        unmatched_df = self.unmatched_df

        # Сохраняем результаты
        try:
            matched_df.to_excel(matched_file, index=False)
            print(f"Совпадения сохранены в файл: {matched_file}.")
        except Exception as e:
            raise ValueError(f"Ошибка при сохранении совпадающих строк: {e}")

        try:
            unmatched_df.to_excel(unmatched_file, index=False)
            print(f"Несовпавшие строки сохранены в файл: {unmatched_file}.")
        except Exception as e:
            raise ValueError(f"Ошибка при сохранении несовпадающих строк: {e}")
