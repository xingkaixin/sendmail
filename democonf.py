import os

class BasicConfig(object):
    # email server
    MAIL_SERVER = os.getenv('testsmtp')
    # MAIL_PORT = 994
    # MAIL_USE_TLS = False
    # MAIL_USE_SSL = True
    MAIL_USERNAME = os.getenv('testmail')
    MAIL_PASSWORD = os.getenv('testmailpw')
    MAIL_DEFAULT_SENDER = MAIL_USERNAME
