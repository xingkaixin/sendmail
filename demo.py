# -*- coding: utf-8 -*-


from flask.ext.mail import (
    Mail,
    Message
)
from flask import (
    Flask,
    render_template
)
from democonf import BasicConfig

mail = Mail()


def create_app():
    app = Flask(__name__)
    app.config.from_object(BasicConfig)
    mail.init_app(app)
    return app


def sendmail():
    title = 'UA TTL'
    reciver = 'dingjiao.lin@baozun.cn'

    shops = []
    shop = {
        'shop': 'TMall',
        'time': '09:00-10:00',
        'order': 10,
        'revenue': 10000,
        'ttl_order': 100,
        'ttl_revenue': 100000
    }
    shops.append(shop)
    shop = {
        'shop': 'JD',
        'time': '09:00-10:00',
        'order': 5,
        'revenue': 5000,
        'ttl_order': 50,
        'ttl_revenue': 50000
    }
    shops.append(shop)

    msg = Message(title)
    msg.add_recipient(reciver)
    msg.html = render_template('demo.html', shops=shops)
    mail.send(msg)

if __name__ == "__main__":
    app = create_app()
    app.app_context().push()
    sendmail()
