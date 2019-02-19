from binance.client import Client
import psycopg2
import json
import pgpasslib
import time
import random
from decimal import Decimal
from tendo import singleton
from datetime import datetime, timedelta

me = singleton.SingleInstance()
passw = pgpasslib.getpass('wytspace.cufjkhoqk6sk.us-east-1.rds.amazonaws.com',
  5432,
  'niexbot',
  'highlander')
conn = psycopg2.connect(
  host='wytspace.cufjkhoqk6sk.us-east-1.rds.amazonaws.com',
  user='highlander',
  dbname='niexbot',
  password=passw,
  port=5432,
  connect_timeout=500)
symbols = [
'TRXBTC',
'XMRBTC',
'XRPBTC',
'ETHBTC',
'ETCBTC',
'XLMBTC',
'LTCBTC',
'RVNBTC'
]
client = Client('xxkeyxx','xxsecretxx')
data_dict=[]
cur = conn.cursor()
insert_cmd = ""
save_balances = False

def open_orders(symbol):
  order_count = 0
  for order in client.get_open_orders(symbol=symbol):
    order_count+=1
  return order_count

btc_cmd = """
  WITH price_value AS (
      SELECT price, symbol, tick_time
        FROM binance_ticker_2019
       WHERE symbol = 'BTCUSDT'
       ORDER BY tick_time DESC
       LIMIT 1)
  SELECT ROUND(((AVG(t.price)-MAX(l.price))/MAX(l.price))*100,8) btc_price_diff,
         CASE WHEN max(l.tick_time) > now() - '10 minutes'::interval THEN True
         ELSE False
         END btc_fresh,
         MAX(l.price) btc_price,
         ROUND(AVG(t.price),8) btc_3_hour_average,
         now()
    FROM binance_ticker_2019 t
    JOIN price_value l ON t.symbol = l.symbol
  WHERE t.tick_time > now() - '3 Hours'::interval
    AND t.symbol = 'BTCUSDT';
   """
cur.execute(btc_cmd)
prev_btc = cur.fetchone()
conn.commit()
print(prev_btc)
price_btc_diff = prev_btc[0]
btc_fresh = prev_btc[1]
btc_price = prev_btc[2]
btc_three_hour_average = prev_btc[3]
nowtime = prev_btc[4]
print('The time is:                 %(t)s' % {"t": nowtime})
print('btc_fresh:                   %(l)s' % {"l": btc_fresh})
print('btc PRICE:                   %(p)s' % {"p": btc_price})
print('btc 3 HOUR:                  %(p)s' % {"p": btc_three_hour_average})
for symbol in symbols:
  print('------------')
  print(symbol)
  print('------------')
  print(datetime.now())
  fresh = False
  select_cmd = """
    WITH price_value AS (
        SELECT price, symbol, tick_time
          FROM binance_ticker_2019
         WHERE symbol = '%(c)s'
         ORDER BY tick_time DESC
         LIMIT 1)
    SELECT ROUND(((AVG(t.price)-MAX(l.price))/MAX(l.price))*100,8) price_diff,
           CASE WHEN max(l.tick_time) > now() - '10 minutes'::interval THEN True
           ELSE False
           END fresh,
           MAX(l.price) "price",
           ROUND(AVG(t.price),8) "3_hour_average",
           now()
      FROM binance_ticker_2019 t
      JOIN price_value l ON t.symbol = l.symbol
    WHERE t.tick_time > now() - '3 Hours'::interval
      AND t.symbol = '%(c)s'
   """ % {"c": symbol}
  cur.execute(select_cmd)
  prev_tick = cur.fetchone()
  price_diff = prev_tick[0]
  fresh = prev_tick[1]
  price = Decimal(prev_tick[2])
  three_hour_average = prev_tick[3]
  conn.commit()
  threeday_cmd = """
    WITH price_value AS (
        SELECT price, symbol, tick_time
          FROM binance_ticker_2019
         WHERE symbol = '%(c)s'
         ORDER BY tick_time DESC
         LIMIT 1)
    SELECT ROUND(((AVG(t.price)-MAX(l.price))/MAX(l.price))*100,8) price_diff,
           ROUND(AVG(t.price),8) btc_3_hour_average
      FROM binance_ticker_2019 t
      JOIN price_value l ON t.symbol = l.symbol
    WHERE t.tick_time > now() - '1 day'::interval
      AND t.symbol = '%(c)s'
   """ % {"c": symbol}
  cur.execute(threeday_cmd)
  prevthree_tick = cur.fetchone()
  three_diff = prevthree_tick[0]
  three_price = prevthree_tick[1]
  conn.commit()
  usdt_symbol = '%(s)sUSDT' % {"s": symbol[:3]}
  usdt_cmd = """
        SELECT price, symbol, tick_time
          FROM binance_ticker_2019
         WHERE symbol = '%(s)s'
         ORDER BY tick_time DESC
         LIMIT 1
     """ % {"s": usdt_symbol}
  cur.execute(usdt_cmd)
  prev_usdt = cur.fetchone()
  conn.commit()
  usdt_price = 0.0
  if prev_usdt is not None:
    usdt_price = prev_usdt[0]
  conn.commit()
  print('fresh:                       %(l)s' % {"l": fresh})
  print('price:                       %(l)s' % {"l": price})
  print('usdt_price:                  %(l)s' % {"l": usdt_price})
  print('3 hour average:              %(l)s' % {"l": three_hour_average})
  print('3 day average:               %(l)s' % {"l": three_price})
  print('price_diff:                  %(l)s' % {"l": price_diff})
  print('three_diff:                  %(p)s' % {"p": three_diff})
  print('price_btc_diff:              %(l)s' % {"l": price_btc_diff})
  print('three_diff > 0.025.... %(t)s > .025' % {"t": three_diff})
  if three_diff > 0.025:
    print('PASSED')
    btc_balance_cmd = """
    SELECT free
      FROM balances_binance
     WHERE asset = 'BTC'
       AND tick_time = (SELECT max(tick_time) FROM balances_binance WHERE asset = 'BTC');"""
    cur.execute(btc_balance_cmd)
    btc_balance = cur.fetchone()[0]
    conn.commit()
    btc_to_spend = Decimal(.7)
    btc_perc_per_trade = Decimal(.025)
    print('BTC Current Holdings:        %(b)s' % {"b": btc_balance})
    print('BTC To Spend on buy:         %(b)s' % {"b": btc_to_spend})
    print('BTC Percent per Trade:       %(b)s' % {"b": btc_perc_per_trade})
    if fresh and btc_fresh:
      if symbol in ('XMRBTC','BCCBTC', 'REPBTC','ETHBTC'):
        amount = round((btc_to_spend*btc_perc_per_trade)/price,3)
      elif symbol.strip() in ('XLMBTC','XRPBTC','ETCBTC','LTCBTC', 'TRXBTC','RVNBTC'):
        amount = round((btc_to_spend*btc_perc_per_trade)/price,0)
      elif symbol in ('XMRBTC'):
        amount = round((btc_to_spend*btc_perc_per_trade)/price,2)
      else:
        amount = round((btc_to_spend*btc_perc_per_trade)/price,4)
      print('amount:                      %(l)s' % {"l": amount})
      hours_since_price_buy_cmd = """
          SELECT EXTRACT(EPOCH FROM current_timestamp-max(created))/3600
            FROM set_trades_binance
           WHERE currencypair = '%(c)s'
             AND not closed
             AND not cancelled;
          """ % {"c": symbol}
      cur.execute(hours_since_price_buy_cmd)
      hours_since_price_buy = cur.fetchone()[0]
      conn.commit()
      if hours_since_price_buy is None:
        hours_since_price_buy = 33
      current_holdings_cmd = """
        SELECT ROUND(t.price*free,8) btc_value
          from balances_binance b
          join (SELECT * FROM binance_ticker_2019 WHERE symbol = '%(c)s' ORDER BY tick_time DESC LIMIT 1) t on symbol = asset || 'BTC'
        where asset = '%(d)s'
        AND b.tick_time = (SELECT MAX(tick_time) FROM balances_binance WHERE asset = '%(d)s');
        """ % {"c": symbol, "d": symbol[:-3]}
      cur.execute(current_holdings_cmd)
      current_holdings_query = cur.fetchone()
      current_holdings = 0.0
      conn.commit()
      if current_holdings_query is not None:
        current_holdings = current_holdings_query[0]
      print('current holdings:              %(c)s BTC' % {"c": round(current_holdings,5)})
      set_trades_binance_count_cmd = """
        SELECT COUNT(*), min(rate)
          FROM set_trades_binance
         WHERE currencypair = '%(c)s'
           AND not closed
           AND not cancelled;
        """ % {"c": symbol}
      cur.execute(set_trades_binance_count_cmd)
      set_trades_binance_count = cur.fetchone()
      conn.commit()
      trade_count = 0
      min_buy = price
      if set_trades_binance_count[1] is not None:
        trade_count = set_trades_binance_count[0]
        min_buy = Decimal(set_trades_binance_count[1])*Decimal(0.92)
      print('trade_count:                  %(l)s' % {"l": trade_count})
      buy_limit = price
      print('buy_limit                         %(b)s' % {"b": buy_limit})
      open_order_count = 0
      open_order_count = open_orders(symbol)
      all_rules_passed = False
      print('open_order_count:                  %(l)s' % {"l": open_order_count})
      print('btc_balance:                       %(b)s' % {"b": btc_balance})
      print('price under buy_limit %(p)s <= %(b)s...' % {"p": price, "b": buy_limit})
      if price <= buy_limit:
        print('PASSED')
        print('1 hours since buy %(h)s....' % {"h": hours_since_price_buy})
        if hours_since_price_buy > 1:
          print('PASSED')
          print('price <= min_buy %(p)s < %(m)s' % {"p": price, "m": min_buy})
          if price <= min_buy:
            print('PASSED')
            print('open_order_count < 1 and trade_count < 5: %(o)s, %(t)s...' % {"o": open_order_count, "t": trade_count})
            if open_order_count < 1 and trade_count < 5:
              print('PASSED')
              all_rules_passed = True
      if all_rules_passed:
        print('buyfuck...............................')
        order_number = 999999999
        buy_price = round(Decimal(price),8)
        try:
          print('AMOUNT:        %(a)s' % {"a":amount})
          print('PRICE:         %(a)s' % {"a":buy_price})
          print('SYMBOL:        %(a)s' % {"a":symbol})
          buy = client.create_order(
            symbol=symbol,
            side=Client.SIDE_BUY,
            type=Client.ORDER_TYPE_MARKET,
            quantity=amount)
          print(buy)
          time.sleep(1)
          order_number = buy["orderId"]
          opened = False
          final_price = buy_price
          if buy["status"] == 'filled':
            opened = True
            final_price = buy['fills'][0]['price']
          insert_cmd = """
            INSERT INTO set_trades_binance (
              currencypair,
              rate,
              amount,
              btc_value,
              usdt_value,
              orderNumber,
              opened,
              trader_id)
            VALUES (
              '%(c)s',
              %(r)s,
              %(a)s,
              %(b)s,
              %(u)s,
              %(o)s,
              %(s)s,
              2);
            """ % {
            "c": symbol,
            "r": final_price,
            "a": amount,
            "b": btc_price,
            "u": usdt_price,
            "o": order_number,
            "s": opened
            }
          cur.execute(insert_cmd)
          rowcount = cur.rowcount
          conn.commit()
          print('set_trade ROWS INSERTED: %(r)s' % {"r": rowcount})
        except Exception as ex:
          print(ex)
  print('BUY PASSED OR FAILED')
  print('CHECK FOR SELL....')
  sell_count_limit = 0
  trade_count = 1
  set_trades_binance_cmd = """
    SELECT *,
    (EXTRACT(epoch from age(now(),created)))::int
      FROM set_trades_binance
     WHERE currencypair = '%(c)s'
       AND not closed
       AND not cancelled
       and trader_id = 2
     ORDER BY id;
    """ % {"c": symbol}
  cur.execute(set_trades_binance_cmd)
  set_trades_binance = cur.fetchall()
  conn.commit()
  set_trade_counter = 0
  for set_trade in set_trades_binance:
    set_trade_counter += 1
    print(set_trade)
    trade_count+=1
    differences_cmd = """
      SELECT (%(b)s - %(lb)s)/%(lb)s btc_difference,
             (%(r)s - %(l)s)/%(l)s trade_difference;
          """ % {
      "b": set_trade[4],
      "lb": btc_price,
      "r": set_trade[2],
      "l": price}
    cur.execute(differences_cmd)
    differences = cur.fetchone()
    conn.commit()
    set_trade_price = Decimal(set_trade[2])
    print('set_trade_price:           %(s)s' % {"s": set_trade_price})
    conn.commit()
    created_time_diff = set_trade[17]
    print('created_time_diff          %(c)s' % {"c": created_time_diff})
    set_trade_price_ten = set_trade_price + (set_trade_price * Decimal(.05))
    if created_time_diff < 7200:
      set_trade_price_ten = set_trade_price + (set_trade_price * Decimal(.01))
    elif created_time_diff < 14400:
      set_trade_price_ten = set_trade_price + (set_trade_price * Decimal(.014))
    elif created_time_diff < 25200:
      set_trade_price_ten = set_trade_price + (set_trade_price * Decimal(.02))
    elif created_time_diff < 50400:
      set_trade_price_ten = set_trade_price + (set_trade_price * Decimal(.03))
    trade_limit = set_trade_price_ten
    print('set_trade_price_ten:           %(s)s' % {"s": set_trade_price_ten})
    print('trade_limit:                   %(s)s' % {"s": trade_limit})
    print('usdt_price:                   %(s)s' % {"s": usdt_price})
    print('set_trade[14]:                   %(s)s' % {"s": Decimal(set_trade[14])})
    if Decimal(set_trade[14]) > .00000001:
      print('(usdt_price-set_trade[14])/set_trade[14] %(p)s' % {"p": (Decimal(usdt_price)-Decimal(set_trade[14]))/Decimal(set_trade[14])})
    sell_amount = round(Decimal(set_trade[3]),3)
    print('sell_amount:               %(s)s' % {"s": sell_amount})
    print('sell_rate:                 %(r)s' % {"r": price})
    if symbol.strip() in ('XRPBTC','TRXBTC'):
      sell_amount = round(sell_amount,0)
    elif symbol.strip() == 'LTCBTC':
      sell_amount = round(sell_amount,2)
    else:
      sell_amount = round(sell_amount,1)
    print('new sell_amount:            %(s)s' % {"s": sell_amount})
    if (trade_limit < price and sell_count_limit < 1):
      print('possible sell')
      print('sellfuck..........................')
      try:
        sell = client.create_order(
          symbol=symbol,
          side=Client.SIDE_SELL,
          type=Client.ORDER_TYPE_MARKET,
          quantity=sell_amount)
        print(sell)
        time.sleep(1)
        close_trade_cmd = """
          UPDATE set_trades_binance
             SET closed = True,
                 btc_close = %(b)s,
                 close = '%(c)s',
                 c_date=now()
           WHERE id = %(o)s
           """ % {"o": set_trade[0], "b": btc_price, "c": price}
        cur.execute(close_trade_cmd)
        rowcount = cur.rowcount
        conn.commit()
        print('set_trade ROWS UPDATED: %(r)s' % {"r": rowcount})
        sell_count_limit += 1
      except Exception as ex:
        print(ex)
    elif Decimal(set_trade[14]) > .00000001:
      if (Decimal(usdt_price)-Decimal(set_trade[14]))/Decimal(set_trade[14]) > .0565:
        print('SELL TO USDT')
        try:
          sell = client.create_order(
            symbol=usdt_symbol,
            side=Client.SIDE_SELL,
            type=Client.ORDER_TYPE_MARKET,
            quantity=sell_amount)
          print(sell)
          time.sleep(1)
          close_trade_cmd = """
            UPDATE set_trades_binance
               SET closed = True,
                   usdt_close = %(u)s,
                   btc_close = %(b)s,
                   close = '%(c)s',
                   c_date=now()
             WHERE id = %(o)s
             """ % {"o": set_trade[0], "u": usdt_price, "b": btc_price, "c": price}
          cur.execute(close_trade_cmd)
          rowcount = cur.rowcount
          conn.commit()
          print('set_trade ROWS UPDATED: %(r)s' % {"r": rowcount})
          sell_count_limit += 1
        except Exception as ex:
          print(ex)
    else:
      if set_trade_price is not None:
        print('Not suitable for sell: %(r)s > %(l)s or sell_count_limit' % {
          "r": trade_limit,
          "l": price})