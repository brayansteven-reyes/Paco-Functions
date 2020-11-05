import mysql.connector
import pathlib
import logging
import os


def get_ssl_cert():
    current_path = pathlib.Path(__file__).parent
    certificate = str (current_path / "BaltimoreCyberTrustRoot.crt.pem")
    logging.info('----------------------- PATH -------------'+ certificate )
    return certificate

def query_db(query, args=(), one=False):
    user_name_db = os.environ["pacoDBUser"]
    password_db = os.environ["pacoDBPassword"]
    cnx = mysql.connector.connect(
        user=user_name_db, 
        password=password_db, 
        host="pacodb.mysql.database.azure.com", 
        port=3306,
        database='paco',
        ssl_ca=get_ssl_cert()
    )
    cur =  cnx.cursor()
    cur.execute(query, args)
    r = [dict((cur.description[i][0], value) \
               for i, value in enumerate(row)) for row in cur.fetchall()]
    cnx.close()
    return (r[0] if r else None) if one else r
