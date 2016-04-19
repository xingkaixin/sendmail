# -*- coding: utf-8 -*-

import cx_Oracle
import os
import arrow
from flask.ext.mail import (
    Mail,
    Message
)
from flask import (
    Flask,
    render_template
)
from democonf import BasicConfig
import time


class DbConn(object):

    db = None

    def __init__(self, conn_str):
        self.db = cx_Oracle.connect(conn_str)

    def close(self):
        self.db.close()
        self.db = None

    def commit(self):
        self.db.commit()

    def rollback(self):
        self.db.rollback()

    def query(self, sql, val={}):
        cur = self.db.cursor()
        cur.execute(sql, val)
        rs = cur.fetchall()
        cur.close()
        return rs

    def querybyGenerator(self, sql, val={}):
        cur = self.db.cursor()
        cur.execute(sql, val)
        while cur:
            yield cur.next()
        cur.close()

    def execute(self, sql, val={}):
        cur = self.db.cursor()
        cur.execute(sql, val)
        counts = cur.rowcount
        cur.close()
        return counts

    def callProc(self, proc, *args):
        cur = self.db.cursor()
        cur.callproc(proc, *args)
        cur.close()
        return True


def updateSeq(dbconn):
    proc = 'PKG_11_SHOP_UA_REPORT_HOUR.SP_MAIN'
    dbconn.callProc(proc, (None, None))
    return True


def run_data():
    DWCONN = os.getenv('DWCONNSTR')
    try:
        dc = DbConn(DWCONN)
        updateSeq(dc)
    except Exception as e:
        print e
        dc.rollback()
    finally:
        dc.close()


def get_exchange_rate():
    DWCONN = os.getenv('DWCONNSTR')
    try:
        dc = DbConn(DWCONN)
        sql  = """
        SELECT CASE
                 WHEN TO_CHAR(MAPPING_FROM1) = '150000021' THEN
                  'HKD:CNY'
                 ELSE
                  'TWD:CNY'
               END EXCHANGE_NAME,
               TO_NUMBER(MAPPING_TO) RATE
          FROM DWH_RPT_REFERENCE_CODE
         WHERE TYPE_ID = 4
           AND TO_CHAR(MAPPING_FROM1) IN ('150000021', '150000022')
        """
        rs = dc.query(sql)
        currencys = []
        for r in rs:
            currency = {
                'name': r[0],
                'rate': r[1]
            }
            currencys.append(currency)
        return currencys

    except Exception as e:
        print e
        dc.rollback()
    finally:
        dc.close()


def get_data(dayid, hour):
    DWCONN = os.getenv('DWCONNSTR')
    try:
        dc = DbConn(DWCONN)
        sql  = """
        SELECT SHOP_NAME,
               PERIOD,
               TRIM(TO_CHAR(SUM(nvl(ORDERS,0)), '999,999,999')) ORDERS,
               TRIM(TO_CHAR(ROUND(SUM(nvl(REVENUE,0))), '999,999,999')) REVENUE,
               TRIM(TO_CHAR(SUM(nvl(TTL_ORDERS,0)), '999,999,999')) TTL_ORDERS,
               TRIM(TO_CHAR(ROUND(SUM(nvl(TTL_REVENUE,0))), '999,999,999')) TTL_REVENUE
          FROM DWH_RPT_UA_HOURLY      A,
               DWH_RPT_REFERENCE_CODE B
         WHERE DAYID = {dayid}
           AND HOUR = {hour}
           AND B.TYPE_ID = 6
           AND A.SHOP_ID = B.MAPPING_FROM1
         GROUP BY B.MAPPING_TO,
                  SHOP_NAME,
                  PERIOD
         ORDER BY B.MAPPING_TO
        """.format(dayid=dayid, hour=hour)
        rs = dc.query(sql)
        shops = []
        for r in rs:
            shop = {
                'shop': r[0],
                'time': r[1],
                'order': r[2],
                'revenue': r[3],
                'ttl_order': r[4],
                'ttl_revenue': r[5],
                'sum': True if r[0] == 'Sum' else False
            }
            shops.append(shop)
        return shops

    except Exception as e:
        print e
        dc.rollback()
    finally:
        dc.close()


mail = Mail()


def create_app():
    app = Flask(__name__)
    app.config.from_object(BasicConfig)
    mail.init_app(app)
    return app


def sendmail():
    title = ''
    recivers = ''

    DWCONN = os.getenv('DWCONNSTR')
    run_data()

    batchday = arrow.utcnow().to('local')
    dayid = batchday.format('YYYYMMDD')
    hour = batchday.format('HH')

    if hour == '00':
        hour = '23'
        dayid = batchday.replace(days=-1).format('YYYYMMDD')
        title += '_{day}_Total'.format(
            day=batchday.replace(days=-1).format('MMM_DD'))
    else:
        hour = batchday.replace(hours=-1).format('HH')

    shops = get_data(dayid, hour)
    currencys = get_exchange_rate()

    msg = Message(title)

    msg.recipients = [r for r in recivers.split(';') if len(r) > 0]
    msg.html = render_template('demo.html', shops=shops, currencys=currencys)
    mail.send(msg)

if __name__ == "__main__":
    for i in range(3):
        try:
            app = create_app()
            app.app_context().push()
            sendmail()
        except:
            time.sleep(60)
        else:
            break
