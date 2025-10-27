from flask import Flask, render_template, jsonify
from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import requests
from datetime import time as t
import time
import json
import warnings
warnings.filterwarnings("ignore")

from utilities import getupdates, etf,merge

app = Flask(__name__)



@app.route('/')
def home():
    return render_template('index.html')

@app.route('/data')
def data():
    code1 = 'GOLDIETF'
    code2 = 'GOLDBEES'
    code3 = 'HDFCGOLD'
    df1 = getupdates()
    df2 = etf(code1)
    df3 = etf(code2)
    df4 = etf(code3)


    df_new = merge(df1,df2,code1)
    df_new = merge(df_new,df3,code2)
    df = merge(df_new,df4,code3)

    #  Convert numpy types and NaN to JSON-safe types
    df = df.applymap(lambda x: x.item() if hasattr(x, 'item') else x)
    df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None})

    return jsonify(df.to_dict(orient='records'))

if __name__ == '__main__':
    app.run()