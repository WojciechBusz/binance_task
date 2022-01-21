import pandas as pd
import requests as r
import operator
from prometheus_client import Gauge, start_http_server
from time import sleep


def BTC_volume():
    url = 'https://api.binance.com/api/v3/ticker/24hr'

    request = r.get(url)
    df = pd.DataFrame(request.json())
    df = df[['symbol', 'volume']]
    df = df[df.symbol.str.contains('BTC')]
    df['volume'] = pd.to_numeric(df['volume'], downcast='float')
    df = df.sort_values(by=['volume'], ascending=False)
    df_final = df.head(5)

    print(df_final)
    return df_final


def USDT_count():
    url = 'https://api.binance.com/api/v3/ticker/24hr'

    request = r.get(url)
    df = pd.DataFrame(request.json())
    df = df[['symbol', 'count']]
    df = df[df.symbol.str.contains('USDT')]
    df['count'] = pd.to_numeric(df['count'], downcast='float')
    df = df.sort_values(by=['count'], ascending=False)
    df_final = df.head(5)

    print(df_final)
    return df_final


def notional_value():
    url = 'https://api.binance.com/api/v3/depth'

    notional_bids_dictionary = {}
    notional_asks_dictionary = {}
    df_symbols = BTC_volume()
    df_symbols = df_symbols['symbol']

    for symbol in df_symbols:
        request = r.get(url, params=dict(symbol=symbol))

        # bids
        df = pd.DataFrame(data=request.json()['bids'], columns=['quantity', 'price'], dtype=float)
        df = df.sort_values(by=['price'], ascending=False)
        df = df.head(200)
        df['notional_value'] = df['quantity'] * df['price']
        notional_bids_dictionary[symbol] = df['notional_value'].sum()

        # asks
        df = pd.DataFrame(data=request.json()['asks'], columns=['quantity', 'price'], dtype=float)
        df = df.sort_values(by=['price'], ascending=False)
        df = df.head(200)
        df['notional_value'] = df['quantity'] * df['price']
        notional_asks_dictionary[symbol] = df['notional_value'].sum()

    print('------------ BIDS ------------')
    for symbol, notional_value in sorted(notional_bids_dictionary.items(), key=operator.itemgetter(1), reverse=True):
        print(symbol, notional_value)

    print('------------ ASKS ------------')
    for symbol, notional_value in sorted(notional_asks_dictionary.items(), key=operator.itemgetter(1), reverse=True):
        print(symbol, notional_value)


def price_spread():
    url = 'https://api.binance.com/api/v3/ticker/bookTicker'

    df_symbols = USDT_count()
    df_symbols = df_symbols['symbol']

    for symbol in df_symbols:
        request = r.get(url, params=dict(symbol=symbol))
        df = pd.DataFrame(request.json(), index=[0])
        df = df[['symbol', 'askPrice', 'bidPrice']]
        df['price_spread'] = float(df['askPrice']) - float(df['bidPrice'])
        df = df[['symbol', 'price_spread']]

        print(df)


def absulute_delta():
    url = 'https://api.binance.com/api/v3/ticker/bookTicker'

    df_symbols = USDT_count()
    df_symbols = df_symbols['symbol']
    price_spread_dictionary_new = {}
    price_spread_dictionary_old = {}
    absolute_delta = {}
    start_http_server(8080)
    g = Gauge('absolute_delta_value', 'Absolute Delta Value of Price Spread', ['symbol'])

    while True:
        for symbol in df_symbols:
            request = r.get(url, params={'symbol': symbol})
            price_spread = request.json()
            price_spread_dictionary_old[symbol] = float(price_spread['askPrice']) - float(price_spread['bidPrice'])

        sleep(10)

        for symbol in df_symbols:
            request = r.get(url, params={'symbol': symbol})
            price_spread = request.json()
            price_spread_dictionary_new[symbol] = float(price_spread['askPrice']) - float(price_spread['bidPrice'])

        for symbol in price_spread_dictionary_old:
            if float(price_spread_dictionary_old[symbol]) >= float(price_spread_dictionary_new[symbol]):
                absolute_delta[symbol] = float(price_spread_dictionary_old[symbol]) - float(
                    price_spread_dictionary_new[symbol])
            else:
                absolute_delta[symbol] = float(price_spread_dictionary_new[symbol]) - float(
                    price_spread_dictionary_old[symbol])

        for symbol in absolute_delta:
            g.labels(symbol).set(absolute_delta[symbol])
        print(absolute_delta)


if __name__ == '__main__':

    #uncomment below functions to run them

    #BTC_volume()
    #USDT_count()
    #notional_value()
    #price_spread()
    #absulute_delta()