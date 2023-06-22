# -*- coding: utf-8 -*-
"""
Created on Sun May 14 20:59:53 2023

@author: Andres
"""

import pandas as pd
import numpy as np

from prefect import flow, task, Flow

@task
def load():
    print("Hello World :)")

"""
with Flow("P1.1 - Hello World") as flow:
    load()

flow.run()
"""
@flow
def flow_caso():
    load()

flow_caso()
