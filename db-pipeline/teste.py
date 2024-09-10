import pandas as pd
import re
import logging
import datetime
import sqlite3

logging.basicConfig(filename='data/flights_pipe_log.log', level=logging.INFO)
logger = logging.getLogger()


def fetch_sqlite_data():# noqa
    try:
        conn = sqlite3.connect("data/NyflightsDB.db")
        logger.info(f'Conexão com banco estabelecida ; {datetime.datetime.now()}')# noqa
    except:# noqa
        logger.error(f'Problema na conexão com banco; {datetime.datetime.now()}')# noqa
    c = conn.cursor()
    c.execute(f"SELECT * FROM nyflights LIMIT 5")
    print(c.fetchall())
    conn.commit()
    conn.close()
    
fetch_sqlite_data()    