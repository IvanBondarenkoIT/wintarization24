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
        Если найдено совпадение, добавляем строку в результат совпадений.
        Остальные строки перемещаются в несовпадения.
        """
        matched_rows = []
        unmatched_rows = unmatched_df.copy()  # Копируем для дальнейшей обработки

        for _, row in unmatched_df.iterrows():
            unmatched_birth_date = row["birth_date"]

            # Поиск совпадений по дате рождения в general_df
            matches = self.general_df[self.general_df["Date of birth"] == unmatched_birth_date]

            if not matches.empty:
                # Если совпадение найдено, добавляем его в результат
                for _, match in matches.iterrows():
                    matched_row = row.copy()  # Копируем строку из unmatched_df
                    matched_row["ut"] = match["ut"]
                    matched_row["Surname"] = match["Surname"]
                    matched_row["Name"] = match["Name"]
                    matched_row["Matched By"] = f"Date of birth: {unmatched_birth_date}"
                    matched_row["georgian_phone"] = match["georgian phone"]  # Добавляем номер телефона
                    matched_rows.append(matched_row)

                # Убираем строку с совпавшей датой рождения из unmatched_df
                unmatched_rows = unmatched_rows[unmatched_rows["birth_date"] != unmatched_birth_date]

        matched_df = pd.DataFrame(matched_rows)

        return matched_df, unmatched_rows

    def process_final_matches(self, matched_df, unmatched_df):
        """
        Проверка совпадений в unmatched_df по georgian_phone_last6
        и добавление ut в соответствующие строки.
        """
        for idx, row in unmatched_df.iterrows():
            unmatched_phone_last6 = row["georgian_phone_last6"]

            phone_match = matched_df[matched_df["georgian_phone_last6"] == unmatched_phone_last6]

            if not phone_match.empty:
                unmatched_df.at[idx, "ut"] = phone_match.iloc[0]["ut"]

        return unmatched_df

    def match_and_save(self, matched_phone_file, unmatched_phone_file, matched_date_file, unmatched_date_file,
                       final_unmatched_file):
        """
        Выполняет все этапы поиска и сохраняет результаты.
        """
        matched_by_phone, unmatched_after_phone = self.match_by_phone()

        matched_by_phone.to_excel(matched_phone_file, index=False)
        unmatched_after_phone.to_excel(unmatched_phone_file, index=False)

        matched_by_birth, unmatched_after_birth = self.match_by_date_of_birth(unmatched_after_phone)

        matched_by_birth.to_excel(matched_date_file, index=False)
        unmatched_after_birth.to_excel(unmatched_date_file, index=False)

        final_unmatched = self.process_final_matches(matched_by_birth, unmatched_after_birth)

        final_unmatched.to_excel(final_unmatched_file, index=False)
        print(f"Финальный файл с несовпавшими сохранён: {final_unmatched_file}.")

    # def match_and_save(self, matched_phone_file, unmatched_phone_file, matched_date_file, unmatched_date_file):
    #     """
    #     Выполняет оба этапа поиска и сохраняет результаты в файлы.
    #
    #     Параметры:
    #         matched_phone_file (str): Путь для сохранения совпавших по телефону.
    #         unmatched_phone_file (str): Путь для сохранения несовпавших по телефону.
    #         matched_date_file (str): Путь для сохранения совпавших по дате рождения.
    #         unmatched_date_file (str): Путь для сохранения несовпавших по всем критериям.
    #     """
    #     # Первый этап: поиск по номеру телефона
    #     matched_by_phone, unmatched_after_phone = self.match_by_phone()
    #
    #     # Сохраняем результаты после первого этапа
    #     try:
    #         matched_by_phone.to_excel(matched_phone_file, index=False)
    #         print(f"Совпадания по телефону сохранены в файл: {matched_phone_file}.")
    #     except Exception as e:
    #         raise ValueError(f"Ошибка при сохранении совпадений по телефону: {e}")
    #
    #     try:
    #         unmatched_after_phone.to_excel(unmatched_phone_file, index=False)
    #         print(f"Несовпавшие по телефону сохранены в файл: {unmatched_phone_file}.")
    #     except Exception as e:
    #         raise ValueError(f"Ошибка при сохранении несовпавших по телефону: {e}")
    #
    #     # Второй этап: поиск по дате рождения среди несовпавших по телефону
    #     matched_by_birth, unmatched_after_birth = self.match_by_date_of_birth(unmatched_after_phone)
    #
    #     # Сохраняем результаты после второго этапа
    #     try:
    #         matched_by_birth.to_excel(matched_date_file, index=False)
    #         print(f"Совпадания по дате рождения сохранены в файл: {matched_date_file}.")
    #     except Exception as e:
    #         raise ValueError(f"Ошибка при сохранении совпадений по дате рождения: {e}")
    #
    #     try:
    #         unmatched_after_birth.to_excel(unmatched_date_file, index=False)
    #         print(f"Несовпавшие по всем критериям сохранены в файл: {unmatched_date_file}.")
    #     except Exception as e:
    #         raise ValueError(f"Ошибка при сохранении несовпавших по всем критериям: {e}")
