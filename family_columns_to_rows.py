import pandas as pd


class FamilyDataProcessor:
    """
    Класс для обработки данных о семьях из всех вкладок файла Excel.

    Атрибуты:
        file_path (str): Путь к файлу Excel с данными о семьях.
        dfs (dict): Словарь с DataFrame для каждой вкладки Excel.
    """

    def __init__(self, file_path):
        """
        Инициализация класса FamilyDataProcessor.

        Параметры:
            file_path (str): Путь к файлу Excel с данными о семьях.
        """
        self.file_path = file_path
        self.dfs = pd.read_excel(file_path, sheet_name=None)  # Загружаем все вкладки как словарь

        for sheet_name, df in self.dfs.items():
            print(f"Вкладка: {sheet_name}")
            print(df.columns.tolist())

    def distribute_family_members(self):
        """
        Распределяет дополнительных членов семьи в новые строки для каждой вкладки.

        Для каждой вкладки выполняет обработку данных:
        - Для каждой семьи создаёт новые строки с общими данными семьи и данными о каждом дополнительном члене.
        - Результат сохраняется в новый файл Excel 'processed_family_data.xlsx'.
        """
        processed_sheets = {}  # Словарь для хранения обработанных данных по вкладкам
        print("Начинаем обработку данных...")

        new_rows = []  # Список для хранения новых строк с данными о членах семьи

        # Проходим по каждой вкладке и её данным
        for sheet_name, df in self.dfs.items():

            print(f"Обрабатываем вкладку: {sheet_name} (количество строк: {len(df)})")


            # Проходим по каждой строке (семье) в DataFrame текущей вкладки
            for index, row in df.iterrows():
                # Сохраняем общие данные семьи (первые 22 колонки)
                common_data = row[:22].tolist()
                new_rows.append(common_data)  # Добавляем новую строку в список с общими данными

                print(f"common_data (длина: {len(common_data)}): {common_data}")

                # Проверяем наличие дополнительных членов семьи
                for i in range(1, 6):  # Предполагаем, что максимум 5 дополнительных членов
                    index = "" if i == 1 else "." + str(i-1)
                    index_plus_one = "." + str(i)
                    if row.get(f'Додати члена сімʼї що перебуває зараз Грузії{index}') == 'Так':
                        # Создаем новую строку для каждого дополнительного члена
                        new_member_data = common_data[:7]  # Копируем общие данные в колонках c 0 до 6

                        new_member_data.extend([
                            row.get(f'Прізвище (за паспортом){index_plus_one}'),
                            row.get(f'Імʼя (за паспортом){index_plus_one}'),
                            row.get(f'Стать{index_plus_one}'),
                            row.get(f'Оберіть закордонний паспорт члена родини. В разі відсутності вкажіть інший документ{index}'),
                            row.get(
                                f'Серія та номер закордонного паспорту або іншого ідентифікаційного документу члена родини{index}'),
                            row.get(f'Дата народження{index_plus_one}'),
                            row.get(f'Дата приїзду до Грузії{index_plus_one}'),
                            row.get(f'Громадянство{index_plus_one}'),
                            row.get(f'Чи відноситься цей член родини до однієї з груп вразливості?{index}')
                        ])
                        # Добавляем оставшиеся колонки из common_data (16–21)
                        new_member_data.extend(common_data[16:])
                        print(f"new_member_data (длина: {len(common_data)}): {common_data}")
                        new_rows.append(new_member_data)  # Добавляем новую строку в список

        # Создаем новый DataFrame для новых строк
        new_df = pd.DataFrame(new_rows)
        print(f"Обработка вкладки '{sheet_name}' завершена. Добавлено {len(new_rows)} новых строк.")
        # , columns=[
        #     'Позначка часу', 'Електронна адреса', 'Телефон Український', 'Телефон Грузинський',
        #     'Місто або населений пункт в якому мешкаєте в Грузії', 'Адреса, за якою мешкаєте в Грузії',
        #     'Кількість осіб в сім\'ї', 'Прізвище', 'Імʼя', 'Стать',
        #     'Оберіть закордонний паспорт', 'Серія та номер закордонного паспорту',
        #     'Дата народження', 'Дата приїзду до Грузії', 'Громадянство',
        #     'Чи відноситесь Ви до однієї з груп вразливості'
        # ])

            # Объединяем оригинальный DataFrame с новыми строками
        # final_df = pd.concat([df, new_df], ignore_index=True)


            # Сохраняем обработанный DataFrame в словарь
        # processed_sheets[sheet_name] = final_df

        # Сохраняем результат в новый файл Excel
        # Сохраняем результат в один файл
        print("Сохраняем данные в файл 'processed_family_data.xlsx'...")
        new_df.to_excel('processed_family_data.xlsx', index=False)
        # print("Сохраняем данные в файл 'processed_family_data.xlsx'...")
        # with pd.ExcelWriter('processed_family_data.xlsx') as writer:
        #     for sheet_name, processed_df in processed_sheets.items():
        #         processed_df.to_excel(writer, sheet_name=sheet_name, index=False)

        print("Данные успешно обработаны и сохранены в 'processed_family_data.xlsx'.")