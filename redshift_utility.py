# Script for redshift database connection and query using script
#Author : Hari om Singh

import pgdb
import logging

import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.info(sys.argv)

def redshift_connection(db_name, db_host, db_port, db_user, db_password):
    # Makes the db connection
    try:
        con = pgdb.Connection(database=db_name, host=db_host,
                              user=db_user, password=db_password, port=db_port)
        logging.info("Database connection succedded")
        return con
    except Exception as e:
        # error while making connection
        logging.error("Error while making database connection \n" + str(e))

def sql_query_result(con,sql_query):
    # retrive the result for the sql query

    try:
        cur = con.cursor()
        query_result=cur.execute(sql_query)
        query_result = cur.fetchall()
        return query_result
    except Exception as e:
        logging.error("Error while retrieving sql query result \n" + str(e))


if __name__=="__main__":

    # DB Details
    db_name = 'database_name'
    db_host = 'mybd.c07934ufxgh.us-east-1.redshift.amazonaws.com'
    db_user = 'database_user_name'
    db_password = 'database_password'
    db_port = 8192

    # Sql Query
    sql='select * from table limit 10 '

    # Db connection
    con=redshift_connection(db_name, db_host, db_port, db_user, db_password)
    sql_query_res=sql_query_result(con,)

    # Iterating to get the sql result
    for values in sql_query_res:
        print(values)
