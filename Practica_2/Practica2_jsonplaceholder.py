# -*- coding: utf-8 -*-
"""
Created on Tue May 16 16:54:25 2023

@author: Andres
"""

import pandas as pd
import numpy as np
import json
import requests
import datetime

from prefect import flow, task

@task(retries=3, retry_delay_seconds=60)
def extract():
    print("** Info: Se Obtendra la respuesta de la API**")
    raw = requests.get("https://jsonplaceholder.typicode.com/posts/1")
    print("**Info: El codigo de respuesta de la API es {}".format(raw.status_code))
    raw = json.loads(raw.text)
    with open(".//raw.json, 'w', enconding='utf-8'") as file:
        json.dump(raw, file, ensure_ascii=False, indent=4)
    return raw

@task(retries=3, retry_delay_seconds=60)
def transform(raw):
    print("** Info: Ejecutando la transformacion**")
    transformed = raw['title']
    with open(".//transformed.json, 'w', encoding='utf-8'") as file:
        json.dump(transformed, file, ensure_ascii=False, indent=4)
    return transformed

@task(retries=3, retry_delay_seconds=60)
def load(transformed):
    print("** Info: Se procedera con la tarea load**")
    print("*****ATENCION*****")
    print("Este es el titulo del primer objeto de la API de posts de Json Placeholder")
    print(str(transformed))
    

    
"""
with Flow("P1.2 - Jsonplaceholder") as flow:
    raw = extract()
    load(raw)
"""

@flow
def flow_caso():
    raw = extract()
    transformed = transform(raw)
    load(transformed)  

flow_caso()