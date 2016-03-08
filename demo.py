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
    DWCONN = os.getenv('DWCONN')
    try:
        dc = DbConn(DWCONN)
        updateSeq(dc)
    except Exception as e:
        print e
        dc.rollback()
    finally:
        dc.close()


def get_data(dayid, hour):
    DWCONN = os.getenv('DWCONN')
    try:
        dc = DbConn(DWCONN)
        sql  = """
        SELECT SHOP_NAME,
               PERIOD,
               TRIM(TO_CHAR(SUM(ORDERS), '999,999,999')) ORDERS,
               ROUND(SUM(REVENUE)) REVENUE,
               TRIM(TO_CHAR(SUM(TTL_ORDERS), '999,999,999')) TTL_ORDERS,
               ROUND(SUM(TTL_REVENUE)) TTL_REVENUE
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
    title = 'UA TTL'
    reciver = 'dingjiao.lin@baozun.cn'

    DWCONN = os.getenv('DWCONN')
    run_data()

    batchday = arrow.utcnow().to('local')
    dayid = batchday.format('YYYYMMDD')
    hour = batchday.format('HH')

    if hour == '00':
        hour = '23'
        dayid = batchday.replace(days=-1).format('YYYYMMDD')

    shops = get_data(dayid, hour)

    msg = Message(title)
    msg.add_recipient(reciver)
    msg.html = render_template('demo.html', shops=shops)
    mail.send(msg)

if __name__ == "__main__":
    app = create_app()
    app.app_context().push()
    sendmail()
