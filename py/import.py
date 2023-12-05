from datetime import datetime
from re import X
import pandas as pd
from sqlalchemy import Engine
from sqlalchemy.engine import URL, create_engine
from calendar import monthrange
import logging
import matplotlib.pyplot as plt


def main(tables: dict):
    """
    docstring
    """

    date_list: list[str] = (
        pd.date_range("2023-01-01", "2023-12-31").strftime("%Y%m%d").tolist()
    )

    count_db = counting_db()

    finaly_table = pd.DataFrame()

    for key, value in tables.items():
        logging.info(f"{key}")
        key = breakdown_by_day(
            value["num_column"], value["name_column"], date_list, count_db
        )
        logging.info(f"{key.size}")
        if finaly_table.empty:
            finaly_table = key
        else:
            finaly_table = finaly_table.merge(
                key, how="left", on=["Уровень 4", "date", "cnt"]
            )

    return finaly_table


def counting_db():
    connection_url = URL.create(
        "mssql+pyodbc",
        host="bi",
        database="RetailAnalytics",
        query={
            "driver": "SQL Server",
            "TrustServerCertificate": "yes",
            "authentication": "ActiveDirectoryIntegrated",
        },
    )

    engine = create_engine(connection_url)

    with engine.connect() as conn:
        cound_db = pd.read_sql_query(
            """ 
        select 
            CAST(date_id AS varchar(15)) as date
            , count(distinct db.db_id) as 'cnt'
        from [RetailAnalytics].[dbo].[lasmart_v_olap_dct_date] dt
            left join [RetailAnalytics].[dbo].[lasmart_v_olap_dct_db] db on CAST(dt.date_time AS date) >= db.OpeningDate 
                                                                        and CAST(dt.date_time AS date) <= case when db.Closing_date is null then '2023-12-31' 
                                                                                                                                            else db.Closing_date
                                                                                                        end
                                                                        and db.Retail_com = 'ТС Победа' 
        where dt.year_id = 2023
        group by date_id
        """,
            conn,
        )

    Engine.dispose

    return cound_db


def breakdown_by_day(
    num_col: list[int], name_col: list[str], date_list: list[str], db: pd.DataFrame
):
    """
    docstring
    """
    tbl = import_data(num_col)

    df_list = []

    for i in tbl.loc[tbl["Уровень 4"] != "Итого"]["Уровень 4"]:
        data = {"Уровень 4": i, "date": date_list}
        df_list.append(pd.DataFrame(data))

    rez_tbl = pd.concat(df_list)
    rez_tbl = rez_tbl.merge(db, how="left", on="date")

    if len(name_col) > 1:
        rez_tbl[name_col[0]] = rez_tbl.apply(calc, axis=1, args=(tbl,))
        rez_tbl[name_col[1]] = rez_tbl.apply(
            lambda x: x[name_col[0]] * (x["cnt"] / 444), axis=1
        )
    else:
        rez_tbl[name_col[0]] = rez_tbl.apply(calc, axis=1, args=(tbl, False))
    return rez_tbl


def import_data(num_col: list[int]):
    rez = pd.read_excel("py\\RTO2023.xlsx", usecols=num_col, skiprows=1)
    rez.columns = [
        "Уровень 4",
        "01",
        "02",
        "03",
        "04",
        "05",
        "06",
        "07",
        "08",
        "09",
        "10",
        "11",
        "12",
    ]
    return rez


def calc(lst: pd.Series, tbl: pd.DataFrame, flag: bool = True):
    mouth = lst.iloc[1][4:6]
    year = lst.iloc[1][0:4]
    if flag:
        re_day = (
            tbl[mouth][tbl["Уровень 4"] == lst.iloc[0]].iloc[0]
            / monthrange(int(year), int(mouth))[1]
        )
    else:
        re_day = tbl[mouth][tbl["Уровень 4"] == lst.iloc[0]].iloc[0]

    return round(re_day, 4)


if __name__ == "__main__":
    logging.basicConfig(filename="log.log", level=logging.INFO)

    TABLES = {
        "rto_grs": {
            "num_column": [_ for _ in range(0, 13)],
            "name_column": ["old_rto_grs", "new_rto_grs"],
        },
        "rto_net": {
            "num_column": [_ for _ in range(0, 27) if _ == 0 or 15 <= _ <= 26],
            "name_column": ["old_rto_net", "new_rto_net"],
        },
        "marj_net_persent": {
            "num_column": [_ for _ in range(0, 40) if _ == 0 or 28 <= _ <= 39],
            "name_column": ["marj_per"],
        },
        "marj_net_money": {
            "num_column": [_ for _ in range(0, 53) if _ == 0 or 41 <= _ <= 52],
            "name_column": ["old_marj_mon", "new_marj_mon"],
        },
        "net_mon": {
            "num_column": [_ for _ in range(0, 66) if _ == 0 or 54 <= _ <= 65],
            "name_column": ["old_net_mon", "new_net_mon"],
        },
    }

    logging.info(datetime.now())
    df = main(TABLES)
    logging.info(datetime.now())
    df.to_excel("test.xlsx", index=False)
