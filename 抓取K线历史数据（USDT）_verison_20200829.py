"""
批量获取历史数据
1. binance 可以获得所有历史数据
2. 火币 只可以获得最近2000根 K线， limit = 2000
3. OKEX, 现货交易 只可以获得最近 200根 K线 ， 但期货 可以任意时间 200根

想要每天自动运行的话，可以参见邢不行公众号这篇文章：https://mp.weixin.qq.com/s/vrv-PniBGxEerJ44AV0jcw

1. 抓取数据，够用就行，别想求全。指定品种、周期。
2. 多注意数据质量，不是说抓下来的数据一定就是干净的。
3. 自己抓取binance的历史数据，火币、ok的历史数据，只能想其他办法，或者...
4. 这就是数据库，别自己额外学数据库。存、用分开。
"""

import pandas as pd
import ccxt
import time
import os
import datetime
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行

# Function: # ===获取begin_date到end_date的每一天，放到date_list中
def get_date_list(begin_date,end_date):
    """
    this function create a standard date list from start date for end date
    :param begin_date:  example:'2018-03-01','2020/03/01' or '03/01/2020'
    :param end_date:   example:'2020/03/04'
    :return:           example:['2020-03-01 00:00:00', '2020-03-02 00:00:00']
    """

    date_list = []
    date = pd.to_datetime(begin_date)
    while date <= pd.to_datetime(end_date):
        date_list.append(str(date))
        date += datetime.timedelta(days=1)
    return date_list

# Function: 获得交易所sybol
def get_symbol_list(exchange):

    """"
    get a symbols list from a exchange,
    cctx and pandas need to be import before run this function
    :param exchange: example: ccxt.okex()
    :return: ['BCV/BTC', 'AAC/USDT', ... , 'KAN/BTC']
    """
    market = exchange.load_markets()
    market = pd.DataFrame(market).T
    symbol_list = list(market['symbol'])
    return symbol_list

# Function: 抓取数据并存储
def save_spot_candle_data_from_exchange(exchange, symbol, time_interval, start_time, path):
    """
    将某个交易所在指定日期指定交易对的K线数据，保存到指定文件夹
    :param exchange: ccxt交易所
    :param symbol: 指定交易对，例如'BTC/USDT'
    :param time_interval: K线的时间周期
    :param start_time: 指定日期，格式为'2020-03-16 00:00:00'
    :param path: 文件保存根目录
    :return:
    """

    # ===对火币的limit做特殊处理
    limit = None
    if exchange.id == 'huobipro':
        limit = 2000

    # ===开始抓取数据
    df_list = []
    start_time_since = exchange.parse8601(start_time)
    end_time = pd.to_datetime(start_time) + datetime.timedelta(days=1)

    while True:
        # 获取数据
        df = exchange.fetch_ohlcv(symbol=symbol, timeframe=time_interval, since=start_time_since, limit=limit)
        # 整理数据
        df = pd.DataFrame(df, dtype=float)  # 将数据转换为dataframe
        # 合并数据
        df_list.append(df)
        # 新的since
        t = pd.to_datetime(df.iloc[-1][0], unit='ms')
        start_time_since = exchange.parse8601(str(t))
        # 判断是否挑出循环
        if t >= end_time or df.shape[0] <= 1:
            break
        # 抓取间隔需要暂停2s，防止抓取过于频繁
        time.sleep(1)

    # ===合并整理数据
    df = pd.concat(df_list, ignore_index=True)
    df.rename(columns={0: 'MTS', 1: 'open', 2: 'high',
                       3: 'low', 4: 'close', 5: 'volume'}, inplace=True)  # 重命名
    df['candle_begin_time'] = pd.to_datetime(df['MTS'], unit='ms')  # 整理时间
    df = df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]  # 整理列的顺序

    # 选取数据时间段
    df = df[df['candle_begin_time'].dt.date == pd.to_datetime(start_time).date()]
    # 去重、排序
    df.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)
    df.sort_values('candle_begin_time', inplace=True)
    df.reset_index(drop=True, inplace=True)

    # ===保存数据到文件
    # 创建交易所文件夹
    if df.shape[0] > 0:
        path = os.path.join(path, exchange.id)
        if os.path.exists(path) is False:
            os.mkdir(path)
        # 创建spot文件夹
        path = os.path.join(path, 'spot')
        if os.path.exists(path) is False:
            os.mkdir(path)
        # 创建日期文件夹
        path = os.path.join(path, str(pd.to_datetime(start_time).date()))
        if os.path.exists(path) is False:
            os.mkdir(path)
        # 拼接文件目录
        file_name = '_'.join([symbol.replace('/', '-'), time_interval]) + '.csv'
        path = os.path.join(path, file_name)
        # 保存数据
        df.to_csv(path, index=False)

#============================================================== 设定 参数 
path = r'C:\Users\dongx\PycharmProjects\coin_2020\data\history_candle_data'

# exchange = ccxt.binance()
# exchange = ccxt.okex()
exchange = ccxt.huobipro()
# symbol_list = get_symbol_list(exchange)

symbol_list = ['BTC/USDT', 'ETH/USDT', 'EOS/USDT', 'LTC/USDT']

time_interval_list = ['5m', '15m']

# 设定 start_time_list
start_time_list = get_date_list('2020-08-17', '2020-08-29')
#=================================================================

# 遍历 start_time, symbol, time_interval,
error_list = []
for start_time in start_time_list:

    for symbol in symbol_list:

        if symbol.endswith('/USDT') is False:
            continue

        for time_interval in time_interval_list:
            print(exchange.id, symbol, time_interval)

            # 抓取数据并且保存
            try:
                save_spot_candle_data_from_exchange(exchange, symbol, time_interval, start_time, path)
            except Exception as e:
                print(e)
                error_list.append('_'.join([exchange.id, symbol, time_interval]))


print(error_list)
