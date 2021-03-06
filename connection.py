import mysql.connector
import pathlib
import logging
import os


def get_ssl_cert():
    current_path = pathlib.Path(__file__).parent
    certificate = str (current_path / "DigiCertGlobalRootG2.crt.pem")
    return certificate

def query_db(query, args=(), one=False):
    user_name_db = os.environ["paco7db7user"]
    password_db = os.environ["paco7db7password"]
    host_db = os.environ["paco7db"]
    cnx = mysql.connector.connect(
        user=user_name_db, 
        password=password_db, 
        host=host_db,
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

def insert_db(query, args=(), one=False):
    user_name_db = os.environ["paco7db7user"]
    password_db = os.environ["paco7db7password"]
    host_db = os.environ["paco7db"]
    cnx = mysql.connector.connect(
        user=user_name_db, 
        password=password_db, 
        host=host_db,
        port=3306,
        database='paco',
        ssl_ca=get_ssl_cert()
    )
    cur =  cnx.cursor()
    cur.execute(query)
    complaint_no = cur.lastrowid
    cnx.commit()
    cnx.close()
    return complaint_no
