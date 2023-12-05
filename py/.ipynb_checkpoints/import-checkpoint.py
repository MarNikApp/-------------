import pandas as pd
import pyodbc


def import_data(num_col: list):
    
    rez = pd.read_excel("data\RTO2023.xlsx", usecols=num_col, skiprows= 1)
    return rez
    

def counting_db():   

    cncx = pyodbc.connect("Driver={SQL Server};"  #{SQL Server Native Client 11.0}
                        "Server=bi;"
                        "Database=RetailAnalytics;"
                        "Trusted_Connection=Yes")

    with cncx:
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
        """, cncx)

    return cound_db


def calc(lst: list):
    
    for trg_lst in lst:
        print(trg_lst)
    


if __name__ == "__main__":
    
    dt_list = pd.date_range('2023-01-01', '2023-12-31').strftime('%Y%m%d').tolist()

    rto_with_nds = import_data(range(0, 13))

    df_list = []

    for i in rto_with_nds['Уровень 4']:
        data = {
            'Уровень 4': i,
            'date': dt_list
        }
        df_list.append(pd.DataFrame(data))

    rez_tbl = pd.concat(df_list)    
    rez_tbl = rez_tbl.merge(counting_db(), how='left', on='date')
    
    rez_tbl.loc[rez_tbl['Уровень 4'] == 'Итого'] 
    rto_with_nds.loc[rto_with_nds['Уровень 4'] == 'Итого']

    rez_tbl['new_column'] = rez_tbl.loc[rez_tbl['Уровень 4'] == 'Итого'] .apply(lambda x: calc(x), axis=1)
    

    #rez_tbl = rez_tbl.merge(rto_with_nds, how='left', on='Уровень 4')
    #print(rez_tbl.loc['Итого','01'])
    
    #rez_tbl['level_koef'] = rez_tbl.apply(lambda x: x / rez_tbl['1'])

    #rez_tbl.to_excel("test.xlsx")


    
