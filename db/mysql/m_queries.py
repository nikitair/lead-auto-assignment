import logging
import json
from m_connector import mysql_connector

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

@mysql_connector
def create_table_tbl_customers(connector):
    curr = connector.cursor()
    curr.execute(
        """
        CREATE TABLE IF NOT EXISTS main.tbl_customers (
            id INT PRIMARY KEY UNIQUE NOT NULL,
            firstname VARCHAR(255) NOT NULL,
            lastname VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL UNIQUE
        );
        """
    )
    logging.info("TABLE tbl_customers CREATED")
    curr.close()


@mysql_connector
def get_all_customers(connector):
    curr = connector.cursor()
    curr.execute(
        "SELECT * FROM main.tbl_customers LIMIT 5"
    )
    logging.info("SELECTING DATA FROM tbl_customers")
    data = curr.fetchall()
    logging.info(data)
    curr.close()
    return data


@mysql_connector
def add_customers(connector, insert_payload: tuple):
    curr = connector.cursor()
    curr.execute(
        """INSERT INTO main.tbl_customers
                (id, firstname, lastname, email)
            VALUES
                ( %s, %s, %s, %s)
            ON DUPLICATE KEY
                UPDATE id = VALUES(id);
    """,
        insert_payload
    )
    connector.commit()
    logging.info(f"INSERTED TO tbl_customers {insert_payload[0]}")
    curr.close()


@mysql_connector
def create_table_tbl_zipcodes(connector):
    curr = connector.cursor()
    curr.execute(
        """
        CREATE TABLE IF NOT EXISTS main.tbl_zipcodes (
            id INT PRIMARY KEY UNIQUE NOT NULL,
            PostalCode VARCHAR(255) NOT NULL UNIQUE,
            City VARCHAR(255) NOT NULL,
            Province VARCHAR(255) NOT NULL
        );
        """
    )
    logging.info("TABLE tbl_zipcodes CREATED")
    curr.close()


@mysql_connector
def get_all_zipcodes(connector):
    curr = connector.cursor()
    curr.execute(
        "SELECT * FROM main.tbl_zipcodes LIMIT 5"
    )
    logging.info("SELECTING DATA FROM tbl_zipcodes")
    data = curr.fetchall()
    logging.info(data)
    curr.close()
    return data


@mysql_connector
def add_zipcode(connector, insert_payload: tuple):
    curr = connector.cursor()
    curr.execute(
        """INSERT INTO main.tbl_zipcodes
                (id, PostalCode, City, Province)
            VALUES
                ( %s, %s, %s, %s)
            ON DUPLICATE KEY
                UPDATE PostalCode = VALUES(PostalCode);
    """,
        insert_payload
    )
    connector.commit()
    logging.info(f"INSERTED TO tbl_zipcodes {insert_payload[0]}")
    curr.close()


if __name__ == "__main__":
    get_all_customers()
    get_all_zipcodes()