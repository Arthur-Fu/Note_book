import pandas as pd
import ccxt
import time
import os
import datetime

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行


def save_spot_candle_data_from_exchange(exchange, symbol, time_interval, start_time, path):
    # ===对火币的limit做特殊处理
    limit = None
    if exchange.id == 'huobipro':
        limit = 2000

    # ===开始抓取数据
    df_list = []
    start_time_since = exchange.parse8601(start_time)
    end_time = pd.to_datetime(start_time) + datetime.timedelta(days=1)

    while True:
        df = exchange.fetch_ohlcv(symbol=symbol, timeframe=time_interval, since=start_time_since, limit=limit)
        df = pd.DataFrame(df, dtype=float)  # 将数据转换为dataframe
        df_list.append(df)
        t = pd.to_datetime(df.iloc[-1][0], unit='ms')
        start_time_since = exchange.parse8601(str(t))
        if t >= end_time or df.shape[0] <= 1:
            break
        # 抓取间隔需要暂停2s，防止抓取过于频繁
        time.sleep(2)

    # ===合并整理数据
    df = pd.concat(df_list, ignore_index=True)
    df.rename(columns={0: 'MTS', 1: 'open', 2: 'high',
                       3: 'low', 4: 'close', 5: 'volume'}, inplace=True)  # 重命名
    df['candle_begin_time'] = pd.to_datetime(df['MTS'], unit='ms')  # 整理时间
    df = df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]  # 整理列的顺序

    df = df[df['candle_begin_time'].dt.date == pd.to_datetime(start_time).date()]
    df.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)
    df.sort_values('candle_begin_time', inplace=True)
    df.reset_index(drop=True, inplace=True)

    # ===保存数据到文件
    path = os.path.join(path, exchange.id)
    if os.path.exists(path) is False:
        os.mkdir(path)
    path = os.path.join(path, 'spot')
    if os.path.exists(path) is False:
        os.mkdir(path)
    path = os.path.join(path, str(pd.to_datetime(start_time).date()))
    if os.path.exists(path) is False:
        os.mkdir(path)
    file_name = '_'.join([symbol.replace('/', '-'), time_interval]) + '.csv'
    path = os.path.join(path, file_name)
    df.to_csv(path, index=False)


def get_symbol_list(exchange):
    market = exchange.load_markets()
    market = pd.DataFrame(market).T
    symbol_list = list(market['symbol'])
    return symbol_list


def get_today_start_time():
    yesterday_date = datetime.date.today() - datetime.timedelta(days=1)
    start_time = str(yesterday_date) + ' 00:00:00'
    return start_time


##============================================ 设定参数
error_list = []
exchange_list = [ccxt.okex(), ccxt.huobipro(), ccxt.binance()]
time_interval_list = ['5m', '15m']
path = r"C:\Users\dongx\PycharmProjects\bit_coin_2020\data\history_candle_data"
start_time = get_today_start_time()
# start_time = '2020-01-01 00:00:00'
##============================================

# 抓取USDT相关的数据对
for exchange in exchange_list:

    # 设定 交易对
    symbols = get_symbol_list(exchange)
    for symbol in symbols:
        if symbol.endswith('/USDT') is False:
            continue
        # 遍历时间周期
        for time_interval in time_interval_list:
            print(exchange.id, symbol, time_interval)

            # 抓取数据并且保存
            try:
                save_spot_candle_data_from_exchange(exchange, symbol, time_interval, start_time, path)
            except Exception as e:
                print(e)
                error_list.append('_'.join([exchange.id, symbol, time_interval]))
print(error_list)