
import asyncio
# from datetime import datetime
import pytz
from datetime import timedelta,time,date
import datetime
from kiteext import KiteExt
import json
import pandas as pd
from time import sleep
import os
import numpy as np
import threading
import requests
start=datetime.datetime.now()
def main1():
    
    print('Running Algo')
    user = json.loads(open('userzerodha.json', 'r').read().rstrip())

    # NOTE contents of above 'userzerodha.json' must be as below
    # {
    #     "user_id": "AB1234",
    #     "password": "P@ssW0RD",
    #     "pin": "123456"
    # }

    kite = KiteExt()
    kite.login_with_credentials(
        userid=user['user_id'], password=user['password'], pin=user['pin'])

    profile = kite.profile()
    print( '\nlogin successful for ',profile['user_name'].upper(),'\n')

    print(profile)
    enctoken = open('enctoken.txt', 'r').read().rstrip()
    print(os.getcwd(),enctoken)
    

    print(enctoken)
    #code whatever logic you want for the running here
    kite.set_headers(enctoken)
    instruments = kite.instruments(exchange="NSE")
    
    true_range_startdt = datetime.datetime.now() - timedelta(days=200)
    true_range_startdt = true_range_startdt.replace(hour = 9,minute=15,second=0)
    true_range_startdt = true_range_startdt.strftime('%Y-%m-%d %H:%M:%S')

    true_range_enddt = datetime.datetime.now() 
    true_range_enddt = true_range_enddt.replace(hour = 15,minute=29,second=59)
    true_range_enddt = true_range_enddt.strftime('%Y-%m-%d %H:%M:%S')

    print(true_range_startdt,true_range_enddt)
    #instrument_df_1 = pd.read_csv("NSE500_tokens.csv")
    instrument_df = pd.read_csv('New_NSE_145.csv')
    print(instrument_df.head())
    DATABASE={}
    for token in instrument_df['token']:
        df_hist_day=kite.historical_data(token,true_range_startdt,true_range_enddt,'day')
        print(df_hist_day.head())
        DATABASE[token] = pd.DataFrame(df_hist_day)
    def levels(ohlc_day):    
        #print(ohlc_day["high"].iloc[-1])
        high = round(ohlc_day["high"].iloc[-1],2)
        low = round(ohlc_day["low"].iloc[-1],2)
        close = round(ohlc_day["close"].iloc[-1],2)
        #print(high,low,close)
        pivot = round((high + low + close)/3,2)
        r1 = round((2*pivot - low),2)
        r2 = round((pivot + (high - low)),2)
        r3 = round((high + 2*(pivot - low)),2)
        s1 = round((2*pivot - high),2)
        s2 = round((pivot - (high - low)),2)
        s3 = round((low - 2*(high - pivot)),2)
        return (pivot,r1,r2,s1,s2)
    def lookback(result,ticker_df):
        lookback=[]
        for dat in result.date:
            index = ticker_df[ticker_df['date']==dat].index.values
            df = ticker_df.iloc[index[0]-20:index[0],:]
            data_20 = df.loc[df['high']<ticker_df.iloc[index[0],2]]
            if(len(data_20)==20):
                #print(dat)
                if abs(ticker_df.iloc[index[0],4]-ticker_df.iloc[index[0],1])<abs(ticker_df.iloc[index[0],2]-ticker_df.iloc[index[0],3])*0.5 and abs(ticker_df.iloc[index[0],2]-ticker_df.iloc[index[0],1])>abs(ticker_df.iloc[index[0],1]-ticker_df.iloc[index[0],3]):
                    lookback.append(dat)
                    #print(dat)    
        return lookback
    DATABASE_DOJI={}
    def inverted_hamm(instrument_df):
        #file1=open("inverted_hammer_stockswithtime.txt","a")
        for token in instrument_df['token']:
            #try:
                print(instrument_df.loc[instrument_df['token']==token,'symbol'])
                #file1.write(instrument_df[instrument_df['token']==token]['symbol'].to_list()[0]+"\n")
                df_hist=kite.historical_data(token,true_range_startdt,true_range_enddt,'15minute')
                df_hist_day=DATABASE[token]
                ticker_df=pd.DataFrame.from_dict(df_hist, orient='columns', dtype=None)
                ticker_df.date=ticker_df.date.astype(str).str[:-6]
                #ticker_df.date=pd.to_datetime(ticker_df.date)

                ticker_df_day=pd.DataFrame.from_dict(df_hist_day, orient='columns', dtype=None)
                ticker_df_day.date=ticker_df_day.date.astype(str).str[:-6]
                #ticker_df_day.date=pd.to_datetime(ticker_df_day.date)
                i=0
                for dat in ticker_df_day.date:
                    if i>0:
                        day = datetime.datetime.strptime(dat,'%Y-%m-%d %H:%M:%S')
                        day = day.replace(hour=0,minute=0,second=0,microsecond=0)
                        starttime = day.replace(hour = 9,minute=14,second=0)
                        starttime = starttime.strftime('%Y-%m-%d %H:%M:%S')
                        endtime = day.replace(hour = 15,minute=29,second=59)
                        endtime = endtime.strftime('%Y-%m-%d %H:%M:%S')
                        #day = day.strftime('%Y-%m-%d')
                        
                        ohlc_day=ticker_df_day.loc[ticker_df_day['date']==str(prev_day)]
                        #ohlc_day=ticker_df.loc[(ticker_df['date']>=starttime) & (ticker_df['date']<endtime)]
                        
                        #print(ohlc_day,day)
                        pivot,r1,r2,s1,s2 = levels(ohlc_day)
                        #print("pivot:",pivot,"r1:",r1,"r2:",r2,"s1:",s1,"s2:",s2)
                        result = ticker_df.loc[((ticker_df['date']>=starttime) & (ticker_df['date']<endtime))&(((ticker_df['high']>pivot) & (ticker_df['low']<pivot)) | ((ticker_df['high']>r1) & (ticker_df['low']<r1)) | ((ticker_df['high']>r2) & (ticker_df['low']<r2)))]
                        #print(day)
                        #print(result)
                        last_20 = lookback(result,ticker_df)
                        #ret = compute_ret(last_20,ticker_df)
                        if(len(last_20)>0):
                            #print("percentage return for open,close",ret)
                            DATABASE_DOJI[token]=result.iloc[np.where(result['date'].isin(last_20))]
                            #file1.writelines(last_20)
                            #file1.write("\n")
                        prev_day = day
                        i+=1
                        
                    else:
                        prev_day=datetime.datetime.strptime(dat,'%Y-%m-%d %H:%M:%S')
                        i+=1
            #except:
                #pass
        #file1.close()
        end=datetime.datetime.now()
        print(end-start)
    def doji_database(instrument_df):
        try:
            for token in instrument_df['token']:
                print(DATABASE_DOJI[token])
        except:
            pass
    while(datetime.now(pytz.timezone('Asia/Kolkata'))<datetime.now(pytz.timezone('Asia/Kolkata')).replace(hour=9,minute=29)):
        pass        
    while(datetime.now(pytz.timezone('Asia/Kolkata'))<datetime.now(pytz.timezone('Asia/Kolkata')).replace(hour=16,minute=50)):
        #    doji_buy_sell()
        ##    i = i+1
                #orb_test()
                #t3=threading.Thread(target=orb_test,args=())
                t1 = threading.Thread(target=inverted_hamm, args=(instrument_df,))
                t2 = threading.Thread(target=doji_database, args=(instrument_df,))

                t1.start()
                t2.start()
                t1.join()
                t2.join()



def fire_and_forget(f):
    def wrapped(*args, **kwargs):
        return asyncio.get_event_loop().run_in_executor(None, f, *args, *kwargs)

    return wrapped


@fire_and_forget
def foo():
    main1()
    print("ALgo run completed")


def main():
    print("Hello world")

    f = open("last_executed.txt", "r")
   
    last_run_date = datetime.datetime.strptime(f.read(), "%d-%m-%y").date()
    if datetime.datetime.now().date() >= last_run_date:
        foo()
        #print("I didn't wait for foo()")

        f = open("last_executed.txt", "w")
        f.write(datetime.datetime.now().strftime("%d-%m-%y"))
        f.close()
    
    return "This is good"


if __name__ == '__main__':
    main()
