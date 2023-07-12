# -*- coding: utf-8 -*-
"""
Created on Tue Jul 11 22:16:00 2023

@author: Andres
"""

# Importacion de librerias a utilizar durante este desarrollo

import yfinance as yf
import pandas as pd
import numpy as np
import requests
import pyodbc
from datetime import date
from prefect import task, Flow
from prefect.tasks.secrets import PrefectSecret

# Export
# Libreria yfinance

@task(log_stdout=True)
def extract(): 
    tickers = ['NVDA', 'TSLA', 'MSFT', 'AMZN', 'AMD', 'INTC'] #['^GSPC', 'GC=F', 'NVDA', 'TSLA']
    raw_dfs = {}
    for ticker in tickers:
        tk = yf.Ticker(ticker)
        raw_df = pd.DataFrame(tk.history(period='1d'))

        raw_df.columns = raw_df.columns.str.lower()
        raw_df = raw_df[['close', 'open', 'high', 'low']]
        raw_dfs[ticker] = raw_df

# BTC coinbase
    response = requests.get('https://api.coinbase.com/v2/prices/spot?currency=USD')
    btc_raw = response.json()
    btc_value = float(btc_raw['data']['amount'])
    btc_dct = {'BTC_exc_usd': btc_value}
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    btc_index = pd.to_datetime([today])
    btc_raw = pd.DataFrame(btc_dct, index=btc_index)

    raw_dfs['BTC_usd'] = btc_raw
    return raw_dfs

@task(log_stdout=True)
def transform(raw_dfs):
    """
    - 
    """
    # Feature engineering
    tickers = ['NVDA', 'TSLA', 'MSFT', 'AMZN', 'AMD', 'INTC']
    for ticker in tickers:
        df = raw_dfs[ticker]

        df['dif_apert_cierre'] = df['open'] - df['close']
        df['rango_dia'] = df['high'] - df['low']
        df['signo_dia'] = np.where(df['dif_apert_cierre'] > 0.0, "+", np.where(df['dif_apert_cierre'] < 0.0, "-", "0"))

        df = df[['close', 'dif_apert_cierre', 'rango_dia', 'signo_dia']]
        df.columns = list(map(lambda x: ticker +'_'+ x, df.columns.to_list()))
        raw_dfs[ticker] = df

    # Agregado
    dfs_list = raw_dfs.values()
    tablon = pd.concat(dfs_list, axis=1)
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    tablon.to_csv("C:\\trace\\{}__post_tranform.csv".format(today))

    return tablon

@task(log_stdout=True)
def load(tablon, credentials):
    """
    - 3.1 - Validación de operación en mercados ese día.
    - 3.2 - Crear tabla si no existe
    - 3.3 - Validar existencia del registro
    - 3.4 - Inserción de registros en tabla
    """
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    
    # Validación de operación en mercados ese día
    num_rows_tablon = len(tablon.index)
    if num_rows_tablon != 1:
        print("Hay conflicto en la actualización de datos. Probablemente no se haya operado en NASDAQ ese día. Revisar trazas.")
        return

    # Create Table if not exists
    nombre_cols_sql = ['[' + col + ']' for col in tablon.columns]
    sql_create_btc_valores = """
        IF NOT EXISTS (SELECT name FROM sys.tables WHERE name = 'btcvalores')
            CREATE TABLE btcvalores (
                [fecha] DATE PRIMARY KEY,
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} VARCHAR,
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} VARCHAR,
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} VARCHAR,
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} VARCHAR,
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} VARCHAR,
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} VARCHAR,
                {} DECIMAL (20, 2)
        )
        """.format(*nombre_cols_sql)

    server = 'tcp:ud-caso-btc-01.database.windows.net' 
    database = 'caso_nasdaq_btc'
    username = 'admin_ud'
    password = credentials 
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)
    cursor = cnxn.cursor()
    cursor.execute(sql_create_btc_valores)
    cnxn.commit()

    # Validar si hay registros para ese día
    query_exists = "SELECT fecha FROM [dbo].[btcvalores] WHERE fecha = '{}'".format(str(today))
    cursor.execute(query_exists)
    row = cursor.fetchone()
    if row:
        print("Ya existe un registro para ese día.")
        return

    # Se realiza insert de la data
    
    tablon.insert(0, 'fecha', today)
    tablon['fecha'] = tablon.index
    for index,row in tablon.iterrows():
        print(row.tolist())
        cursor.execute('INSERT INTO dbo.btcvalores VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', row.tolist())
        cnxn.commit()
    cursor.close()
    cnxn.close()

with Flow("ETL BTC") as flow:
    raw_dfs = extract()
    tablon = transform(raw_dfs)
    secret = PrefectSecret("pwd_sql")
    load(tablon = tablon, credentials = secret)

flow.run()