import sqlite3
import os
import pandas as pd
from dotenv import load_dotenv
import assets.utils as utils
from assets.utils import logger
import datetime
import logging


load_dotenv()


def data_clean(df,metadados):
    df["data_voo"] = pd.to_datetime(df[['year', 'month', 'day']]) 
    df = utils.null_exclude(df, metadados["cols_chaves"])
    df = utils.convert_data_type(df, metadados["tipos_originais"])
    df = utils.select_rename(df, metadados["cols_originais"], metadados["cols_renamed"])
    df = utils.string_std(df, metadados["std_str"])
    df.loc[:,"datetime_partida"] = df.loc[:,"datetime_partida"].str.replace('.0', '')
    df.loc[:,"datetime_chegada"] = df.loc[:,"datetime_chegada"].str.replace('.0', '') 
    for col in metadados["corrige_hr"]:
        lst_col = df.loc[:,col].apply(lambda x: utils.corrige_hora(x))
        df[f'{col}_formatted'] = pd.to_datetime(df.loc[:,'data_voo'].astype(str) + " " + lst_col)
    
    logger.info(f'Saneamento concluído; {datetime.datetime.now()}')
    return df

def feat_eng(df):
    data = df.copy()
    data["tempo_voo_esperado"] = (data["datetime_chegada_formatted"] - data["datetime_partida_formatted"]) / pd.Timedelta(hours=1)# 
    data["dia_semana"] = data["data_voo"].dt.day_of_week 
    #data["distancia"] = utils.recupera_dist(origem, destino, table, db)# 
    data["horario"] = data.loc[:, "datetime_partida_formatted"]
    data["tempo_voo_hr"] = data["tempo_voo"]/60
    data["atraso"] = data["tempo_voo_hr"] - data["tempo_voo_esperado"]
    
    return data
  

def save_data_sqlite(df):
    try:
        conn = sqlite3.connect("data/NyflightsDB.db")
        logger.info(f'Conexão com banco estabelecida ; {datetime.datetime.now()}')# noqa
    except:# noqa
        logger.error(f'Problema na conexão com banco; {datetime.datetime.now()}')# noqa
    c = conn.cursor()# noqa
    df.to_sql('nyflights', con=conn, if_exists='replace')
    conn.commit()
    logger.info(f'Dados salvos com sucesso; {datetime.datetime.now()}')
    conn.close()

def fetch_sqlite_data(table):# noqa
    try:
        conn = sqlite3.connect("data/NyflightsDB.db")
        logger.info(f'Conexão com banco estabelecida ; {datetime.datetime.now()}')# noqa
    except:# noqa
        logger.error(f'Problema na conexão com banco; {datetime.datetime.now()}')# noqa
    c = conn.cursor()
    c.execute(f"SELECT * FROM {table} LIMIT 5")
    print(c.fetchall())
    conn.commit()
    conn.close()


if __name__ == "__main__":
    logger.info(f'Inicio da execução ; {datetime.datetime.now()}')
    metadados  = utils.read_metadado(os.getenv('META_PATH'))
    df = pd.read_csv(os.getenv('DATA_PATH'),index_col=0)
    df = data_clean(df, metadados)
    print(df.head())
    utils.null_check(df, metadados["null_tolerance"])
    utils.keys_check(df, metadados["cols_chaves"])
    df = feat_eng(df)
    #save_data_sqlite(df)
    fetch_sqlite_data(metadados["tabela"][0])
    logger.info(f'Fim da execução ; {datetime.datetime.now()}')