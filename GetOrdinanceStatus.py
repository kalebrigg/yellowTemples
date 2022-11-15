# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 14:16:07 2019

@author: winwardn
"""

import sys
import os
import requests
import pandas as pd
import time

sys.path.append(r'V:\FHSS-JoePriceResearch\tools')
from FamilySearch1 import FamilySearch
fs = FamilySearch('nrwinward','Bluejay77',os.getcwd(),'in.csv','out.csv',auth=True)
token = fs.Authenticate()

#this is all that should need changed
os.chdir(r'C:\Users\krigg23\Desktop\dataFinder-master')
inFile = "microEditsTODO.csv"
outFile = "n_ordinances.csv"

#none of this should need modified
def getOrdinanceStatus(row):
    try:
        pid = row['pid']
        url = 'https://www.familysearch.org/service/tree/tree-data/v8/person/{}/summary?locale=en&includeTempleRollupStatus=true'.format(pid)
        response = session.get(url)
        if response.status_code == 429:
            wait = (int(response.headers['Retry-After'])*1.1)
            print('Throttled, waiting {0: .1f} seconds!'.format(wait))
            time.sleep(wait)
            response = session.get(url)
        data = response.json()
        return data['templeRollupStatus']
    except Exception as e:
        print(e)
        return "error"

    
df = pd.read_csv(inFile)
with requests.Session() as session:  
    session.headers.update({'Authorization':f'Bearer {token}',
                    'Accept':'application/json',
                    'Content-type': 'application/json'})   
    df['ordinanceStatus'] = df.apply(lambda row: getOrdinanceStatus(row), axis = 1)

df.to_csv(outFile,index=False)

