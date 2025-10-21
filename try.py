from tvDatafeed import TvDatafeed, Interval
import pandas as pd
from datetime import time
import os





def getupdates():
    
    tv = TvDatafeed(username="endbeginner992000", password="resetyourpassword@1234")
    nifty_index_data = tv.get_hist(
      symbol='XAUINRG',
      exchange='FX_IDC',
      interval=Interval.in_5_minute,
      n_bars=10000000
  )


    df = nifty_index_data.reset_index()
    df = df.sort_values(by='datetime', ascending=True)
    df = df.drop(['symbol'], axis=1)

    df_latest_row = df[-1:-2:-1]

    #Filter rows where time is exactly 3:30 PM
    df_ = df[df['datetime'].dt.time == time(15, 30)].reset_index(drop=True)
    df_

    #Filter rows where time is exactly 9 AM
    df_9am = df[df['datetime'].dt.time == time(9, 00)][1:].reset_index(drop=True)
    df_['current_day'] = df_9am['datetime']
    df_['pre-market-price'] = df_9am['open']
    df_ = df_.drop(labels=['high','low','close','volume'],axis=1)
    df_ = df_.rename(columns={'open':'prev_close'})
    df = df_.sort_values('datetime',ascending=False)
    df = df.reset_index(drop=True)

    df.loc[0,['pre-market-price','current_day']] = [df_latest_row.iloc[0]['close'],df_latest_row.iloc[0]['datetime']]

    df['delta'] = round((df['pre-market-price'] - df['prev_close'])/df['prev_close']*100,2)
    df['delta'] = df['delta'].astype(str)+'%'

    return df

print(getupdates())
