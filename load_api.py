# Make connection to API and load data into the dataframe

def get_klines(api_key, api_secret, start_date, end_date,limit = 200, interval = 'hours', symbol= "BTCUSD"):

    '''
    returns the klines of symbol between start_date and end_date

    start_date (YYYY-MM-DD): The earliest date that needs to be collected
    end_date (YYYY-MM-DD): The final date collected
    symbol: Symbol according to the bybit denomination of the bitcoin
    '''
    from pybit.unified_trading import HTTP
    import pandas as pd
    from datetime import datetime
    import numpy as np 


    # Transform the date to the right format
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")


    if interval == 'hours':
        interval = 60
        date_range = pd.date_range(start_date, end_date, freq='8D')\
    

    date_range = date_range.astype(np.int64)/10**6


    # Start session with Bibit
    session = HTTP(
        testnet=True,
        api_key=api_key,
        api_secret=api_secret,
    )
    bybit_return = []
    for i in range(len(date_range)-1):
        bybit_return.append(session.get_kline(
            category="inverse",
            symbol=symbol,
            interval=interval,
            start=date_range[i],
            end=date_range[i+1],
            limit = limit
        ))
    

    # Format the data into a dataframe
    df = np.vstack([bybit['result']['list'] for bybit in bybit_return])

    df = pd.DataFrame(df)
    df.columns = ['kline','open', 'high', 'low', 'close', 'volume','turnover']
    df['kline'] = pd.to_datetime(df['kline'].astype('float'), unit = 'ms',)

    df.set_index('kline', inplace = True)
    df= df.astype('float')
    df[ 'currency'] = bybit_return[0]['result']['symbol']
    df = df.sort_index()
    return df


def get_portfolio():
    from pybit.unified_trading import HTTP
    session = HTTP(
    testnet=True,
    api_key=api_key,
    api_secret=api_secret,
    )
    positions = session.get_positions(
    category='linear',
    symbol="BTCUSD",
    )

    return positions