import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine
from typing import Set, Dict, Any


class SLA:
    """
    Класс для генерации отчёта по SLA (Service Level Agreement).

    Атрибуты:
    ----------
    STATUS_DICT : Dict[int, str]
        Словарь соответствия статусов транзакций.

    start : str
        Начальная дата выборки транзакций.

    stop : str
        Конечная дата выборки транзакций.

    path : str
        Путь к файлу с транзакциями.

    conn_uri : str
        URI для подключения к базе данных.

    engine : sqlalchemy.engine.base.Engine
        Объект SQLAlchemy для управления соединениями с базой данных.

    conn : sqlalchemy.engine.base.Connection
        Текущее соединение с базой данных.

    df_zai : pd.DataFrame
        Данные из файла Excel с транзакциями.

    ID : Set[Any]
        Уникальные идентификаторы транзакций из Excel-файла.

    result : pd.DataFrame
        Итоговый отчёт с объединёнными данными.
    """
    STATUS_DICT: Dict[int, str] = {
        2: "В обработке", 3: "Провально", 4: "Succeed", 8: "Фейл по нашей вине",
        10: "Отменено", 11: "Истек", 12: "Обнуление", 13: "Отмена до реквизитов"
    }
    
    def __init__(self, start: str, stop: str, path: str):
        """
        Инициализирует объект SLA.

        Параметры:
        ----------
        start : str
            Начальная дата выборки транзакций.

        stop : str
            Конечная дата выборки транзакций.

        path : str
            Путь к файлу Excel с транзакциями.
        """
        self.conn_uri: str = ''  # Здесь необходимо указать строку подключения к базе данных
        self.engine: sqlalchemy.engine.base.Engine = create_engine(self.conn_uri)
        self.conn: sqlalchemy.engine.base.Connection = self.engine.connect().execution_options()
        
        self.df_zai: pd.DataFrame = self.load_transaction(path)
        self.start: str = start
        self.stop: str = stop
        self.ID: Set[Any] = self.get_id_zai()
        self.result: pd.DataFrame = self.load_admin()

    def load_transaction(self, path: str) -> pd.DataFrame:
        """
        Загружает файл Excel с транзакциями.

        Параметры:
        ----------
        path : str
            Путь к Excel-файлу.

        Возвращает:
        ----------
        pd.DataFrame
            Данные из Excel-файла.
        """
        try:
            return pd.read_excel(path)
        except FileNotFoundError:
            print("Файл не найден.")
            return pd.DataFrame()

    def get_id_zai(self) -> Set[Any]:
        """
        Извлекает уникальные идентификаторы транзакций из Excel-файла.

        Возвращает:
        ----------
        Set[Any]
            Множество уникальных идентификаторов транзакций.
        """
        first_column = self.df_zai.iloc[:, 0]
        return set(first_column)

    def load_admin(self) -> pd.DataFrame:
        """
        Формирует итоговый отчёт, объединяя данные из базы данных и Excel-файла.

        Возвращает:
        ----------
        pd.DataFrame
            Итоговый DataFrame с объединёнными данными.
        """
        # Преобразуем self.ID в строку для корректного формата SQL
        id_list = tuple(self.ID)
        
        query_combined = f"""
            SELECT 
                io.external_mid_id, 
                CAST(io.amount AS Float64) AS admin_amount,
                io.status_id AS admin_status,
                io.mid_id AS gateway_admin_name,
                toString(io.accepted_at) AS accepted_at, 
                mm.display_name AS currency_name,  -- Заменяем currency_id на display_name
                'invoice' AS transaction_type
            FROM merch.invoice_order AS io
            LEFT JOIN merch.currency_list AS cl ON CAST(io.currency_id AS Int64) = CAST(cl.id AS Int64)
            LEFT JOIN merch.mid AS mm ON CAST(io.mid_id AS Int64) = CAST(mm.id AS Int64)
            WHERE io.external_mid_id IN {id_list} AND
            io.accepted_at BETWEEN '{self.start}' AND '{self.stop}'
            
            UNION ALL
            
            SELECT 
                wo.external_mid_id, 
                CAST(wo.amount AS Float64) AS admin_amount,
                wo.status_id AS admin_status,
                wo.mid_id AS gateway_admin_name,
                toString(wo.accepted_at) AS accepted_at, 
                cl.display_name AS currency_name,  -- Заменяем currency_id на display_name
                'withdraw' AS transaction_type
            FROM merch.withdraw_order AS wo
            LEFT JOIN merch.currency_list AS cl ON CAST(wo.currency_id AS Int64) = CAST(cl.id AS Int64)
            LEFT JOIN merch.mid AS mm ON CAST(wo.mid_id AS Int64) = CAST(mm.id AS Int64)
            WHERE wo.external_mid_id IN {id_list} AND
            wo.accepted_at BETWEEN '{self.start}' AND '{self.stop}'
        """
        df_combined = pd.read_sql(query_combined, con=self.conn)
        df_combined['admin_status'] = df_combined['admin_status'].map(self.STATUS_DICT)
        df_combined['gateway_status'] = None
        df_combined = df_combined[df_combined["admin_status"] != "Истек"]
        df_combined = df_combined.drop_duplicates()
        df_combined.sort_values("gateway_admin_name", inplace=True)
        df_combined.to_excel(f'SLA.xlsx', index=False)
        
        return df_combined


# Пример использования
start_upload = '2024-11-12 00:00:00'  # Начальная дата
stop_upload = '2024-11-19 00:00:00'   # Конечная дата
path_to_file = "файл.xlsx"             # Путь к Excel-файлу

report = SLA(
    start=start_upload, 
    stop=stop_upload,
    path=path_to_file
)
