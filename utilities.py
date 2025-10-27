from flask import Flask, render_template, jsonify
from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import requests
from datetime import time as t
import time
import json


def getupdates():
  tv = TvDatafeed()
  nifty_index_data = tv.get_hist(
      symbol='XAUINRG',
      exchange='FX_IDC',
      interval=Interval.in_5_minute,
      n_bars=10000000
  )


  df_old = nifty_index_data.reset_index()
  df_old = df_old.sort_values(by='datetime', ascending=True)
  df_old = df_old.drop(['symbol'], axis=1)
  df_old['datetime'] = df_old['datetime'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)

  df_latest_row = df_old[-1:-2:-1]

  #Filter rows where time is exactly 3:30 PM
  df_ = df_old[df_old['datetime'].dt.time == t(15, 25)].reset_index(drop=True)
  df_

  #Filter rows where time is exactly 9 AM
  df_9am = df_old[df_old['datetime'].dt.time == t(9, 15)][1:].reset_index(drop=True)

  df_['Today'] = df_9am['datetime']
  df_["Today's end"] = df_old[df_old['datetime'].dt.time == t(15, 25)][1:].reset_index(drop=True)['datetime']
  df_["Today's_end_price"] =df_old[df_old['datetime'].dt.time == t(15, 25)][1:].reset_index(drop=True)['close']

  df_['market-price'] = df_9am['open']
  df_ = df_.drop(labels=['high','low','open','volume'],axis=1)
  df_ = df_.rename(columns={'close':'prev_close'})
  df = df_.sort_values('datetime',ascending=False)
  df = df.reset_index(drop=True)

  df.loc[0,['market-price','Today']] = [df_latest_row.iloc[0]['close'],df_latest_row.iloc[0]['datetime']]

  df['market delta'] = round((df['market-price'] - df['prev_close'])/df['prev_close']*100,2)
  df['market delta'] = df['market delta'].astype(str)+'%'
  df['Day change'] = round((df["Today's_end_price"]-df['prev_close'])/df['prev_close']*100,2)
  df['Day change'] = df['Day change'].astype(str)+'%'

  df=df.rename(columns={'datetime':'Yesterday'})

  df = df[["Yesterday","prev_close","market-price","Today","market delta","Today's end","Today's_end_price","Day change"]]

  return df

def etf(code):
  etf = code
  endTimeInMillis = int(time.time())*1000
  intervalInMinutes = '5'
  startTimeInMillis = int(time.time())*1000 - 2334959000


  headers = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
      "Accept-Language": "en-US,en;q=0.9",
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
      "Referer": "https://google.com",
      "Connection": "keep-alive"
  }


  url = f'https://groww.in/v1/api/charting_service/v2/chart/delayed/exchange/NSE/segment/CASH/{etf}?endTimeInMillis={endTimeInMillis}&intervalInMinutes={intervalInMinutes}&startTimeInMillis={startTimeInMillis}'
  response = requests.get(url,headers=headers)
  data = response.json()
  df= pd.DataFrame(data['candles'],columns=['starttime','open','high','low','close','na'])
  df.drop(['na'],axis=1,inplace=True)
  df.starttime = pd.to_datetime(df.starttime,unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
  df_ = df.copy()
  df_latest_row = df_[-1:-2:-1].reset_index(drop=True)
  df_9am = df_[df_['starttime'].dt.time == t(15, 25)][1:]
  df_330pm = df_[df_['starttime'].dt.time == t(15, 25)][:-1]
  df = df_330pm[['starttime','close']].reset_index(drop=True)
  df['endtime'] = df_9am[['starttime']].reset_index(drop=True)
  df['close_new'] = df_9am[['close']].reset_index(drop=True)
  df = df.sort_values('endtime',ascending=False).reset_index(drop=True)
  df.loc[0,["endtime",'close_new']] = [df_latest_row.iloc[0]['starttime'],df_latest_row.iloc[0]['close']]

  df[f'delta|{code}'] = round((df['close_new']-df['close'])/df['close']*100,2)
  df[f'delta|{code}'] = df[f'delta|{code}'].astype(str)+'%'
  df.rename(columns={'starttime':f'Yesterday|{code}','endtime':"Today's end",'close_new':f"Today's_end_price|{code}"},inplace=True)
  df =df.reset_index(drop=True)

  return df

def merge(df1,df2,code):
    df_try = df1.merge(df2,on="Today's end",how='left')

    j = df_try.to_json(orient='records',date_format='iso')

    data = json.loads(j)

    return rectify_holidays_change(data,code)


def rectify_holidays_change(data,code):
  n = data[::-1]
  off_flag = False
  prev_close_data = None
  for i in n:
    if i[f'Yesterday|{code}'] == None and off_flag == True:
      pass

    elif i[f'Yesterday|{code}'] == None:
      off_flag = True
      prev_close_data = i['prev_close']

    elif off_flag==True:
      i['prev_close'] = prev_close_data
      off_flag = False
      prev_close_data = None
      i['market delta'] = str(round(((i['market-price'] - i['prev_close'])/i['prev_close'] * 100),2))+'%'
      i['Day change'] = str(round(((i["Today's_end_price"] - i['prev_close'])/i['prev_close'] * 100),2))+'%'


  n = n[::-1]

  df3 = pd.DataFrame(n)
  df3['Yesterday'] = pd.to_datetime(df3['Yesterday'])
  df3[f'Yesterday|{code}'] = pd.to_datetime(df3[f'Yesterday|{code}'])
  df3["Today's end"] = pd.to_datetime(df3["Today's end"])
  df3['Today'] = pd.to_datetime(df3['Today'])

  return df3.drop([f'Yesterday|{code}','close',f"Today's_end_price|{code}"],axis=1)

