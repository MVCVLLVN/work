import os
import re
import shutil
import logging
import sqlalchemy
import pandas as pd
from pathlib import Path
import clickhouse_driver
import clickhouse_sqlalchemy
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text


shutil.rmtree("saveАвто")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if logger.hasHandlers():
    logger.handlers.clear()

# Очистка глобальных обработчиков
logging.root.handlers = []

formatter = logging.Formatter(
    fmt='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)


class EveryDayAuto:
    CLIENTS = {
        1282: {"columns_filter": "accepted_at", "plus_time": 0},
        1130: {"columns_filter": "accepted_at", "plus_time": 0},
        606: {"columns_filter": "accepted_at", "plus_time": 0},
        1359: {"columns_filter": "accepted_at", "plus_time": 0},
        741: {"columns_filter": "accepted_at", "plus_time": 0},
        1160: {"columns_filter": "accepted_at", "plus_time": 0},
        1235: {"columns_filter": "accepted_at", "plus_time": 3},
        1352: {"columns_filter": "accepted_at", "plus_time": 3},
    }

    STATUS_DICT = {
        2: "В обработке",
        3: "Провально",
        4: "Succeed",
        8: "Фейл по нашей вине",
        10: "Отменено",
        11: "Истек",
        12: "Reversal",
        13: "Отмена до реквизитов",
    }
    
    FILE_PREFIXES = {
        'P2PW': 'Report_Transactions',
        'WW': 'Report_Transactions_UZS',
        'LR': 'Report_Transactions_LR',
        'P2P': 'Report_Transactions_NEW'
    }

    def __init__(self):
        """
        Инициализация класса EveryDayAuto.

        Выполняет следующие действия:
        - Устанавливает подключение к базе данных ClickHouse.
        - Создаёт пустой DataFrame для последующей обработки.
        - Инициализирует начальные переменные для хранения состояния.
        - Запускает метод `activate` для обработки данных.

        Атрибуты:
        ----------
        conn_uri : str
            Строка подключения к базе данных ClickHouse.

        engine : sqlalchemy.engine.base.Engine
            Объект SQLAlchemy для управления соединением с базой данных.

        conn : sqlalchemy.engine.base.Connection
            Текущее соединение с базой данных.

        DF : pd.DataFrame
            Пустой DataFrame для хранения обработанных данных.

        col_filter : str
            Столбец, по которому будет производиться фильтрация данных.

        client_name : str
            Название текущего клиента.

        delta_time : int
            Временной сдвиг (в часах) для корректировки временных данных.
        """
        logger.info("Инициализация EveryDayAuto")

        self.conn_uri: str = ''
        self.engine: sqlalchemy.engine.base.Engine = create_engine(self.conn_uri)
        self.conn: sqlalchemy.engine.base.Connection = self.engine.connect().execution_options()

        self.DF: pd.DataFrame = pd.DataFrame()
        self.col_filter: str = ""
        self.client_name: str = ""
        self.delta_time: int = 0

        self.activate()

    def get_current_day(self, delta_time: int = 0) -> Tuple[datetime, datetime]:
        """
        Получение временного диапазона для отчёта.

        Метод определяет начало вчерашнего дня и начало текущего дня с учётом временного сдвига.

        Параметры:
        ----------
        delta_time : int, optional
            Временной сдвиг в часах (по умолчанию 0). 
            Может быть использован для настройки времени начала дня.

        Возвращает:
        ----------
        Tuple[datetime, datetime]
            - start_of_yesterday (datetime): Дата и время начала вчерашнего дня с учётом delta_time.
            - stop_of_yesterday (datetime): Дата и время начала текущего дня с учётом delta_time.

        Логирование:
        ----------
        Логирует временные диапазоны для отладки.
        """
        today = datetime.now()
        yesterday = today - timedelta(days=3 if today.weekday() == 0 else 1)
        start_of_yesterday = yesterday.replace(hour=delta_time, minute=0, second=0, microsecond=0)
        stop_of_yesterday = today.replace(hour=delta_time, minute=0, second=0, microsecond=0)

        logger.info(f"Начало вчерашнего дня: {start_of_yesterday}, начало сегодняшнего дня: {stop_of_yesterday}")
        
        return start_of_yesterday, stop_of_yesterday

    def create_df(self, idClient: int, start_of_yesterday: datetime, stop_of_yesterday: datetime) -> pd.DataFrame:
        """
        Создание DataFrame с данными клиента из базы.

        Метод формирует SQL-запрос для выборки данных из таблиц `invoice_order` и `withdraw_order`, 
        объединяет их, и возвращает результат в виде DataFrame.

        Параметры:
        ----------
        idClient : int
            Идентификатор клиента, для которого производится выборка данных.

        start_of_yesterday : datetime
            Начало временного диапазона выборки (вчерашний день).

        stop_of_yesterday : datetime
            Конец временного диапазона выборки (сегодняшний день).

        Возвращает:
        ----------
        pd.DataFrame
            DataFrame с объединёнными данными транзакций (invoice + withdraw).

        Исключения:
        ----------
        - Если возникает ошибка при выполнении SQL-запроса, возвращается пустой DataFrame.
        - Логируются ошибки выполнения запроса.
        """
        logger.info(f"Создание DataFrame для клиента {idClient}")
        query_combined = f"""
            SELECT io.external_id AS external_id, 
                  io.client_order_id AS client_order_id, 
                  io.client_id AS client_id, 
                  io.currency_id AS currency_id, 
                  toString(io.created_at) AS created_at, 
                  toString(io.accepted_at) AS accepted_at,  
                  CAST(io.initial_amount AS Float64) AS amount, 
                  CAST(io.amount AS Float64) AS accepted_amount, 
                  CAST(io.service_commission AS Float64) AS service_commission, 
                  CAST(io.mid_commission AS Float64) AS mid_commission, 
                  io.status_id AS status_name, 
                  io.comment AS comment, 
                  io.type AS type, 
                  io.provodka AS provodka, 
                  io.mid_id,
                  c.name AS client_name, 
                  cl.display_name AS currency_name, 
                  'invoice' AS transaction_type
            FROM merch.invoice_order io
            LEFT JOIN merch.client c ON CAST(io.client_id AS Int64) = CAST(c.id AS Int64)
            LEFT JOIN merch.currency_list cl ON CAST(io.currency_id AS Int64) = CAST(cl.id AS Int64)
            WHERE io.client_id = {idClient}
            AND io.accepted_at BETWEEN '{start_of_yesterday}' AND '{stop_of_yesterday}'
            
            UNION ALL
            
            SELECT wo.external_id AS external_id, 
                  wo.client_order_id AS client_order_id, 
                  wo.client_id AS client_id, 
                  wo.currency_id AS currency_id, 
                  toString(wo.created_at) AS created_at, 
                  toString(wo.accepted_at) AS accepted_at, 
                  CAST(wo.amount AS Float64) AS amount, 
                  CAST(wo.amount AS Float64) AS accepted_amount, 
                  CAST(wo.service_commission AS Float64) AS service_commission, 
                  CAST(wo.mid_commission AS Float64) AS mid_commission, 
                  wo.status_id AS status_name, 
                  wo.comment AS comment, 
                  wo.type AS type, 
                  wo.provodka AS provodka, 
                  wo.mid_id AS mid_id,
                  c.name AS client_name, 
                  cl.display_name AS currency_name, 
                  'withdraw' AS transaction_type
            FROM merch.withdraw_order wo
            LEFT JOIN merch.client c ON CAST(wo.client_id AS Int64) = CAST(c.id AS Int64)
            LEFT JOIN merch.currency_list cl ON CAST(wo.currency_id AS Int64) = CAST(cl.id AS Int64)
            WHERE wo.client_id = {idClient}
            AND wo.accepted_at BETWEEN '{start_of_yesterday}' AND '{stop_of_yesterday}'
        """   
        try:
            # Выполнение SQL-запроса и создание DataFrame
            df_combined = pd.read_sql(query_combined, con=self.conn)
            logger.info(f"Данные для клиента {idClient} успешно загружены")
        except Exception as e:
            # Логирование ошибки и возврат пустого DataFrame
            logger.error(f"Ошибка при загрузке данных для клиента {idClient}: {e}")
            df_combined = pd.DataFrame()
        
        return df_combined


    
    def set_new_col(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Обработка и предобработка данных.

        Метод выполняет следующие операции:
        - Присваивает значения из `accepted_amount` в `amount` для транзакций с типом `withdraw` или с нулевым значением `amount`.
        - Преобразует значения столбца `status_name` согласно словарю `STATUS_DICT`.
        - Вычисляет общий размер комиссии (`overall_commission`) для каждой строки.
        - Удаляет дублирующиеся строки на основе столбца `external_id`.
        - Сортирует строки по указанному столбцу `self.col_filter`.
        - Сбрасывает индексы DataFrame.

        Параметры:
        ----------
        df : pd.DataFrame
            Исходный DataFrame, содержащий транзакции.

        Возвращает:
        ----------
        pd.DataFrame
            DataFrame после выполнения всех операций предобработки.

        Исключения:
        ----------
        - Если возникает ошибка при обработке данных, логируется сообщение об ошибке и исключение пробрасывается дальше.
        """
        logger.info("Начало обработки данных")
        try:
            # Замена значений в столбце 'amount' для транзакций типа 'withdraw' или с нулевым значением
            df.loc[df['transaction_type'] == 'withdraw', 'amount'] = df['accepted_amount']
            df.loc[df['amount'] == 0, 'amount'] = df['accepted_amount']
            
            # Преобразование статусов транзакций
            df['status_name'] = df['status_name'].map(self.STATUS_DICT)
            
            # Вычисление общей комиссии
            df["overall_commission"] = (
                (df['mid_commission'] + df['service_commission']) * (df['accepted_amount'] / 100)
            ).round(3)
            
            # Удаление дубликатов и сортировка данных
            df.drop_duplicates('external_id', inplace=True)
            df.sort_values(self.col_filter, inplace=True)
            
            # Сброс индексов
            df.reset_index(drop=True, inplace=True)
            
            logger.info("Обработка данных завершена")
        except Exception as e:
            logger.error(f"Ошибка обработки данных: {e}")
            raise
        return df


    
    def remove_microseconds(self, value: Union[str, None]) -> Union[str, None]:
        """
        Удаление миллисекунд из строки даты.

        Метод принимает строку с датой и временем, удаляет из неё миллисекунды (если они присутствуют), 
        и возвращает обработанную строку. Если значение `None` или пустое, возвращается `None`.

        Параметры:
        ----------
        value : Union[str, None]
            Строка, содержащая дату и время, или значение `None`.

        Возвращает:
        ----------
        Union[str, None]
            Обработанная строка без миллисекунд или `None`, если входное значение отсутствует.

        Исключения:
        ----------
        - Если `value` не является строкой или содержит некорректный формат, метод может вернуть необработанное значение.
        """
        if pd.isna(value):
            return None
        # Убираем миллисекунды и оставляем только дату и время
        cleaned_value = re.sub(r'\.\d+', '', value)
        return cleaned_value

        
    def filter_data_time(self, df: pd.DataFrame, filter_column: str) -> pd.DataFrame:
        """
        Фильтрация данных по времени.

        Метод выполняет фильтрацию данных по заданному столбцу времени (`filter_column`) в диапазоне `self.start_date` 
        и `self.end_date`. Также метод обрабатывает временные данные, добавляет временной сдвиг (`plus_time`) 
        для клиентов, очищает миллисекунды и преобразует значения в формат datetime.

        Параметры:
        ----------
        df : pd.DataFrame
            DataFrame с исходными данными для фильтрации.

        filter_column : str
            Название столбца, по которому выполняется фильтрация (например, 'accepted_at').

        Возвращает:
        ----------
        pd.DataFrame
            DataFrame, отфильтрованный по времени и статусу транзакции ('Succeed').

        Логирование:
        ----------
        - Логирует успешное выполнение фильтрации и имя клиента.
        - Логирует ошибки при выполнении фильтрации.

        Исключения:
        ----------
        - Если возникает ошибка в процессе фильтрации, метод логирует сообщение об ошибке и пробрасывает исключение.
        """
        logger.info("Фильтрация данных по времени")

        try:
            # Получение уникального client_id и временного сдвига (plus_time)
            un_c = df.client_id.unique()[0]
            plus = self.CLIENTS.get(un_c).get("plus_time")

            # Удаление миллисекунд и преобразование столбцов времени
            df['created_at'] = df['created_at'].apply(self.remove_microseconds)
            df['accepted_at'] = df['accepted_at'].apply(self.remove_microseconds)
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce') + pd.to_timedelta(plus, unit='hours')
            df['accepted_at'] = pd.to_datetime(df['accepted_at'], errors='coerce') + pd.to_timedelta(plus, unit='hours')

            # Фильтрация по временным границам
            self.DF = df[
                (df[filter_column] >= self.start_date) & 
                (df[filter_column] < self.end_date)
            ]

            # Определение уникального имени клиента
            unique_clients = self.DF['client_name'].unique()
            self.client_name = unique_clients[0] if len(unique_clients) == 1 else "DEFAULT"

            # Фильтрация данных по статусу транзакции
            self.DF = self.DF[self.DF['status_name'] == 'Succeed']

            logger.info(f"Фильтрация завершена. Клиент: {self.client_name}")
        except Exception as e:
            logger.error(f"Ошибка фильтрации данных: {e}")
            raise

        return self.DF


        
    def result_save(self) -> None:
        """
        Сохранение результатов в файл с учётом `plus_time` для клиентов.

        Метод проверяет наличие данных в DataFrame, фильтрует необходимые столбцы и сохраняет данные в формате CSV или Excel. 
        Формат и имя файла определяются по клиенту (`client_name`).

        Условия:
        ----------
        - Если DataFrame пуст, метод завершает выполнение и логирует предупреждение.
        - Если в DataFrame отсутствуют необходимые столбцы, метод завершает выполнение и логирует ошибку.
        - Если клиент указан в `FILE_PREFIXES`, данные сохраняются в формате CSV. 
          Для остальных клиентов данные сохраняются в формате Excel.

        Логирование:
        ----------
        - Логируются успешное сохранение результатов или возникающие ошибки.

        Исключения:
        ----------
        - Если возникают ошибки при сохранении файла, они логируются, и исключение пробрасывается.

        Параметры:
        ----------
        Нет параметров.

        Возвращает:
        ----------
        None
        """
        if self.DF.empty:
            logger.warning(f"DataFrame пустой. Пропуск сохранения для клиента: {self.client_name}")
            return

        folder_path = Path('saveАвто')
        folder_path.mkdir(exist_ok=True)

        selected_columns = [
            "external_id", "client_order_id", "client_name", "currency_name",
            "created_at", "accepted_at", "amount", "overall_commission",
            "status_name", "transaction_type", "comment", "accepted_amount"
        ]

        # Проверка наличия всех необходимых столбцов
        missing_columns = [col for col in selected_columns if col not in self.DF.columns]
        if missing_columns:
            logger.error(f"Отсутствуют столбцы: {missing_columns}")
            return

        # Получение client_id из DataFrame
        client_id = self.DF['client_id'].iloc[0] if 'client_id' in self.DF.columns else None
        if client_id is None:
            logger.error("Не удалось определить client_id из DataFrame")
            return

        # Применение `plus_time` и добавление 1 секунды
        for col in ['created_at', 'accepted_at']:
            self.DF[col] = pd.to_datetime(self.DF[col]) + pd.Timedelta(seconds=1)
            if col in self.DF.columns and self.client_name in self.FILE_PREFIXES:
                # Преобразование в Unix Timestamp для клиентов Monetix
                self.DF[col] = self.DF[col].astype(int) // 10**9

        formatted_date = f"{self.start_date.strftime('%d.%m.%Y')}-{self.end_date.strftime('%d.%m.%Y')}"

        # Определение имени файла и формата
        if self.client_name in self.FILE_PREFIXES:
            # Сохранение в формате CSV для клиентов Monetix
            file_prefix = self.FILE_PREFIXES[self.client_name]
            file_name = f"{file_prefix} {formatted_date}.csv"
            file_path = folder_path / file_name
            try:
                self.DF[selected_columns].to_csv(file_path, index=False)
                logger.info(f"Результат сохранён в формате CSV: {file_path}")
            except Exception as e:
                logger.error(f"Ошибка сохранения CSV файла: {e}")
                raise
        else:
            # Сохранение в формате Excel для остальных клиентов
            file_name = f"{self.client_name}_{formatted_date}_2024.xlsx"
            file_path = folder_path / file_name
            try:
                self.DF[selected_columns].to_excel(file_path, index=False, sheet_name='Данные')
                logger.info(f"Результат сохранён в формате Excel: {file_path}")
            except Exception as e:
                logger.error(f"Ошибка сохранения Excel файла: {e}")
                raise


    
    def activate(self) -> None:
        """
        Активирует процесс сверки данных для всех клиентов.

        Метод последовательно выполняет следующие шаги для каждого клиента:
        - Устанавливает фильтруемый столбец (`columns_filter`) из конфигурации клиента.
        - Получает временной диапазон для фильтрации (`start_date`, `end_date`).
        - Загружает данные из базы данных в DataFrame с помощью `create_df`.
        - Если данных нет, логирует предупреждение и переходит к следующему клиенту.
        - Выполняет предобработку данных с использованием методов `set_new_col` и `filter_data_time`.
        - Сохраняет результат обработки с помощью `result_save`.

        Логирование:
        ----------
        - Логируется начало и завершение сверки для каждого клиента.
        - Логируется отсутствие данных для клиента.

        Исключения:
        ----------
        - Ошибки, возникающие в методах обработки данных, логируются и пробрасываются.

        Параметры:
        ----------
        Нет параметров.

        Возвращает:
        ----------
        None
        """
        for client_id, config in self.CLIENTS.items():
            logger.info(f"\nНачало сверки для клиента {client_id}")

            # Установка фильтруемого столбца из конфигурации клиента
            self.col_filter = config["columns_filter"]

            # Получение временного диапазона для фильтрации
            self.start_date, self.end_date = self.get_current_day()

            # Загрузка данных из базы данных
            df = self.create_df(client_id, self.start_date, self.end_date)
            if df.empty:
                logger.warning(f"Нет данных для клиента {client_id}")
                continue

            # Предобработка данных
            df = self.set_new_col(df)
            df = self.filter_data_time(df, self.col_filter)

            # Сохранение результата
            self.result_save()

            logger.info(f"Сверка завершена для клиента {client_id}")



AUTO = EveryDayAuto()
