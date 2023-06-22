# -*- coding: utf-8 -*-
"""
Created on Sun May 14 21:00:07 2023

@author: Andres
"""

import pandas as pd
import numpy as np
import json
import requests

from prefect import flow, task

@task
def extract():
    response = requests.get("https://jsonplaceholder.typicode.com/posts")
    response = response.json()
    
    return response

@task
def load(response):
    output = response[0]['title']
    print("*****ATENCION*****")
    print("Este es el titulo del primer objeto de la API de posts de Json Placeholder")
    print(str(output))
    
    
"""
with Flow("P1.2 - Jsonplaceholder") as flow:
    raw = extract()
    load(raw)
"""

@flow
def flow_caso():
    raw = extract()
    load(raw)

flow_caso()