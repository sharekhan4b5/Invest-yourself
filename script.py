
import asyncio
# from datetime import datetime
import pytz
from datetime import timedelta,date
import datetime
import time
from kiteext import KiteExt
import json
import config
import pandas as pd
import os
import numpy as np
import threading
import requests
start=datetime.datetime.now()
def main1():
    
    print('Running Algo')
    #user = json.loads(open('userzerodha.json', 'r').read().rstrip())

    # NOTE contents of above 'userzerodha.json' must be as below
    # {
    #     "user_id": "AB1234",
    #     "password": "P@ssW0RD",
    #     "pin": "123456"
    # }

    
    
    #user = json.loads(open('userzerodha.json', 'r').read().rstrip())
    #print(user)
    kite = KiteExt()
    #kite.login_with_credentials(
    #userid=user['user_id'], password=user['password'], twofa=user['twofa'])
    kite.login_with_credentials(userid=config.username, password=config.password, pin=config.pin)
    with open('enctoken.txt', 'w') as wr:
          wr.write(kite.enctoken)

    print(kite.profile())
    enctoken = open('enctoken.txt', 'r').read().rstrip()
    print(os.getcwd(),enctoken)
    #kite = KiteExt()
    print(enctoken)
    kite.set_headers(enctoken)
    ##print(kite.profile())
    instruments = kite.instruments(exchange="NSE")
    
 
    true_range_startdt = datetime.datetime.now() - timedelta(days=5)
    startdt = true_range_startdt
    true_range_startdt = true_range_startdt.replace(hour = 9,minute=15,second=0)
    true_range_startdt = true_range_startdt.strftime('%Y-%m-%d %H:%M:%S')

    true_range_enddt = datetime.datetime.now() - timedelta(days=2)
    enddt= true_range_enddt
    true_range_enddt = true_range_enddt.replace(hour = 15,minute=29,second=59)
    true_range_enddt = true_range_enddt.strftime('%Y-%m-%d %H:%M:%S')

    print(true_range_startdt,true_range_enddt)
    instrument_df = pd.read_csv("New_NSE_145.csv")

    #token_name=instrument_df[instrument_df['token']==1207553].symbol

    # %%
    today=(datetime.datetime.now() - timedelta(days=2)).date()
    today=today.strftime('%Y-%m-%d')

    # %%
    DATABASE={}
    for token in instrument_df['token']:
        df_hist_day=kite.historical_data(token,true_range_startdt,true_range_enddt,'day')
        #print(data)
        DATABASE[token] = df_hist_day
    end=datetime.datetime.now()
    print(end-start)


    # %%
    def telegram_bot_sendtext(bot_message):
        bot_token = '1772481683:AAGCtefuhSLBeRtNdFxRYkLX-a9eG8H5qyY'
        bot_chatID = '-1001253024203'
        send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

        response = requests.get(send_text)

        return response.json()


    #test = telegram_bot_sendtext("Testing Telegram bot")
    #print(test)


    # %%
    def levels(ohlc_day):    
        """returns pivot point and support/resistance levels"""
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


    # %%
    def lookback(result,ticker_df):
        lookback=[]
        for dat in result.date:
            index = ticker_df[ticker_df['date']==dat].index.values
            df = ticker_df.iloc[index[0]-20:index[0],:]
            data_20 = df.loc[df['high']<ticker_df.iloc[index[0],2]]
            if(len(data_20)==20):
                #print(dat)
                #1: open
                #2: high
                #3: low
                #4: close
                if abs(ticker_df.iloc[index[0],4]-ticker_df.iloc[index[0],1])<abs(ticker_df.iloc[index[0],2]-ticker_df.iloc[index[0],3])*0.25 and abs(ticker_df.iloc[index[0],2]-ticker_df.iloc[index[0],1])>abs(ticker_df.iloc[index[0],1]-ticker_df.iloc[index[0],3]):
                    lookback.append(dat)
                    #print(dat)    
        return lookback


    # %%
    def compute_ret(last_20,ticker_df):
        res = {}
        for dat in last_20:
           index = ticker_df[ticker_df['date']==dat].index.values
           open = ticker_df.iloc[index[0]+1,1]
           prev_close=ticker_df.iloc[index[0]+1,4]
           currentclose = ticker_df.iloc[index[0],4]
           currentopen = ticker_df.iloc[index[0],1]
           #current = ticker_df.iloc[index[0],1]

           #low < currentlow
           close = ticker_df.iloc[index[0]+7,4]
           pctchange = (close-open)/open
           #print(pctchange)
           if prev_close<currentclose and prev_close<currentopen:
             res[dat]=pctchange
        return res

    # %%
    def doji_bs_order(capital_per_tick,DATABASE_DOJI,token):
        global tokens_name_doji

        ihtokens= list(DATABASE_DOJI.keys())
        #print(tokenName_NEG)
        #print(DATABASE_NEG)
        print("Inverted hammer tokens",ihtokens)
        if(len(ihtokens)==0):
          return
        print(DATABASE_DOJI[token])
        #for token in ihtokens:
        token_name=instrument_df[instrument_df['token']==token].symbol
        token_name=list(token_name)
        token_name=token_name[0]
        currentToken = token
        tkn_name="NSE:"+token_name
        orderBook = kite.orders()
        checkBuy = 1
        checkSell = 1
        buyOrderCount = 0
        sellOrderCount = 0
        lastOrderType = None
        lastOrderTs = datetime.datetime(2021,1,1)
        time.sleep(1)
        for x in orderBook:
            time.sleep(1)
            if(x['tradingsymbol']==token_name and x['status']=='COMPLETE'):
                if(x['transaction_type']=='BUY'):
                    buyOrderCount += 1
                    if(x['order_timestamp']>lastOrderTs):
                        lastOrderTs = x['order_timestamp']
                        lastOrderType = 'BUY'
                elif(x['transaction_type']=='SELL'):
                    sellOrderCount += 1
                    if(x['order_timestamp']>lastOrderTs):
                        lastOrderTs = x['order_timestamp']
                        lastOrderType = 'SELL'

        if(sellOrderCount>buyOrderCount):
            checkSell = 0
        elif(sellOrderCount<buyOrderCount):
            checkBuy = 0
        else:
            if(lastOrderType=='BUY'):
                checkSell=0
            elif(lastOrderType=='SELL'):
                checkBuy=0
            ordersLeft = [x['order_id'] for x in kite.orders() if (x['status']=='OPEN' or x['status']=='PENDING' or x['status']=='TRIGGER PENDING') and x['tradingsymbol']==token_name]
            for x in ordersLeft:
                kite.cancel_order(variety=kite.VARIETY_REGULAR,order_id=x)
                print("order cancelled")    
            time.sleep(1)    
        #print(currentToken,buyOrderCount,sellOrderCount)
        time.sleep(1)
        ltp_price=kite.ltp(tkn_name)
        lst_prc = ltp_price[tkn_name]['last_price']  
        print("DOJI BUY SELL Stock name",tkn_name,"last price ",lst_prc,"buy sell order count",buyOrderCount,sellOrderCount)
        #qty = round(capital_per_tick/lst_prc)
        qty=1
        print(qty,capital_per_tick,lst_prc)
        if(lst_prc<DATABASE_DOJI[currentToken]['low'].iloc[-1] and checkSell):
                    #DATABASE[currentToken]['sellonce'] = 0
                    qty=1
                    #print('o2',currentToken,token_name,qty)
                    """orderId = kite.place_order(variety=kite.VARIETY_REGULAR, 
                                exchange=kite.EXCHANGE_NSE, 
                                tradingsymbol=token_name, 
                                transaction_type=kite.TRANSACTION_TYPE_SELL, 
                                quantity=qty, 
                                product=kite.PRODUCT_MIS, 
                                order_type=kite.ORDER_TYPE_MARKET, 
                                price=None, 
                                validity=None, 
                                disclosed_quantity=None, 
                                trigger_price=None, 
                                squareoff=None,
                                stoploss=None,
                                trailing_stoploss=None)
                    time.sleep(1)
                    kite.place_order(tradingsymbol=token_name,
                                    exchange=kite.EXCHANGE_NSE,
                                    transaction_type=kite.TRANSACTION_TYPE_BUY,
                                    quantity=qty,
                                    price = round(lst_prc*0.995,1),
                                    order_type=kite.ORDER_TYPE_LIMIT,
                                    product=kite.PRODUCT_MIS,
                                    variety=kite.VARIETY_REGULAR)
                    time.sleep(1)
                    kite.place_order(tradingsymbol=token_name,
                                    exchange=kite.EXCHANGE_NSE,
                                    transaction_type=kite.TRANSACTION_TYPE_BUY,
                                    quantity=qty,
                                    order_type=kite.ORDER_TYPE_SLM,
                                    price=round(DATABASE_DOJI[currentToken]['high'].iloc[-1],1),
                                    trigger_price = round(DATABASE_DOJI[currentToken]['high'].iloc[-1],1),
                                    product=kite.PRODUCT_MIS,
                                    variety=kite.VARIETY_REGULAR)
                    print(orderId)"""
                    #text_message = token_name+" sold at lst_prc "+str(lst_prc)+" Target "+ str(round(lst_prc*0.991,1))+" SL "+ str(round(lst_prc*1.015,1))
                    text_message = ""
                    #print(text_message)
                    test = telegram_bot_sendtext(text_message)
                    print("Stock name",tkn_name,"last price ",lst_prc,"buy sell order count",buyOrderCount,sellOrderCount)
        elif(lst_prc>DATABASE_DOJI[currentToken]['high'].iloc[-1] and checkBuy):
            #DATABASE[currentToken]['buyonce'] = 0
            #qty = round(capital_per_tick/kite.ltp(token_name))
            qty=1
            print(qty,capital_per_tick,lst_prc)
            print('o1',currentToken,token_name,qty)
            """orderId = kite.place_order(variety=kite.VARIETY_REGULAR, 
                        exchange=kite.EXCHANGE_NSE, 
                        tradingsymbol=token_name, 
                        transaction_type=kite.TRANSACTION_TYPE_BUY, 
                        quantity=qty, 
                        product=kite.PRODUCT_MIS, 
                        order_type=kite.ORDER_TYPE_MARKET, 
                        price=None, 
                        validity=None, 
                        disclosed_quantity=None, 
                        trigger_price=None, 
                        squareoff=None, 
                        stoploss=None, 
                        trailing_stoploss=None)
            time.sleep(1)
            ltp_price=kite.ltp(tkn_name)
            lst_prc = ltp_price[tkn_name]['last_price']
            kite.place_order(tradingsymbol=token_name,
                            exchange=kite.EXCHANGE_NSE,
                            transaction_type=kite.TRANSACTION_TYPE_SELL,
                            quantity=qty,
                            price = round(lst_prc*1.005,1),
                            order_type=kite.ORDER_TYPE_LIMIT,
                            product=kite.PRODUCT_MIS,
                            variety=kite.VARIETY_REGULAR)
            time.sleep(1)
            kite.place_order(tradingsymbol=token_name,
                            exchange=kite.EXCHANGE_NSE,
                            transaction_type=kite.TRANSACTION_TYPE_SELL,
                            quantity=qty,
                            order_type=kite.ORDER_TYPE_SLM,
                            price=round(DATABASE_DOJI[currentToken]['low'].iloc[-1],1),
                            trigger_price=round(DATABASE_DOJI[currentToken]['low'].iloc[-1],1),
                            product=kite.PRODUCT_MIS,
                            variety=kite.VARIETY_REGULAR)
            print(orderId)"""
            #text_message="Buy order executed for the stock "+" "+token_name+"last price "+str(lst_prc)
           # text_message = token_name+" Bought at lst_prc "+str(lst_prc)+" Target "+ str(round(lst_prc*1.009,1))+" SL "+ str(round(lst_prc*0.985,1))
            text_message = ""
            #print(text_message)
            test = telegram_bot_sendtext(text_message)
            print(test)    


    # %%
    DATABASE_DOJI={}
    def inverted_hamm(instrument_df):
        #file1=open("inverted_hammer_stockswithtime.txt","a")



        for token in instrument_df['token']:
            #try:
                #print(instrument_df.loc[instrument_df['token']==token,'symbol'])
                #file1.write(instrument_df[instrument_df['token']==token]['symbol'].to_list()[0]+"\n")
                df_hist=kite.historical_data(token,true_range_startdt,true_range_enddt,'15minute')
                #print(df_hist)
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
                            if(token in list(DATABASE_DOJI.items())):
                                if(result.iloc[np.where(result['date'].isin(last_20))]!=DATABASE_DOJI[token]):
                                    DATABASE_DOJI[token]=result.iloc[np.where(result['date'].isin(last_20))]
                                    #print(DATABASE_DOJI[token].iloc[-1].date[:10],today)
                                    if(DATABASE_DOJI[token].iloc[-1].date[:10]==today):
                                        doji_bs_order(1000,DATABASE_DOJI,token)
                                        print("stock name: ",token," IH candle time: ",DATABASE_DOJI[token].iloc[-1].date," low: ",DATABASE_DOJI[token].iloc[-1].low," high: ",DATABASE_DOJI[token].iloc[-1].high)
                            else:
                                DATABASE_DOJI[token]=result.iloc[np.where(result['date'].isin(last_20))]
                                #print(DATABASE_DOJI[token].iloc[-1].date[:10],today)
                                if(DATABASE_DOJI[token].iloc[-1].date[:10]==today):
                                    doji_bs_order(1000,DATABASE_DOJI,token)
                                    print("stock name: ",token," IH candle time: ",DATABASE_DOJI[token].iloc[-1].date," low: ",DATABASE_DOJI[token].iloc[-1].low," high: ",DATABASE_DOJI[token].iloc[-1].high)

                            #file1.writelines(last_20)
                            #file1.write("\n")
                        prev_day = day
                        i+=1

                    else:
                        #print(dat)
                        prev_day=datetime.datetime.strptime(dat,'%Y-%m-%d %H:%M:%S')
                        i+=1
            #except:
                #pass
        #file1.close()

        end=datetime.datetime.now()
        print(end-start)

    # %%




    # %%
    while(datetime.datetime.now(pytz.timezone('Asia/Kolkata'))<datetime.datetime.now(pytz.timezone('Asia/Kolkata')).replace(hour=9,minute=16)):
            pass        
    while(datetime.datetime.now(pytz.timezone('Asia/Kolkata'))<datetime.datetime.now(pytz.timezone('Asia/Kolkata')).replace(hour=21,minute=50)):
        #    doji_buy_sell()
        ##    i = i+1
                #orb_test()
                #t3=threading.Thread(target=orb_test,args=())
                #if(not (datetime.datetime.now().minute%15)):

                    t1 = threading.Thread(target=inverted_hamm, args=(instrument_df,))
                    #t2 = threading.Thread(target=doji_bs_order, args=(1000,))

                    t1.start()
                    #t2.start()
                    t1.join()
                    break
                # t2.join()






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
