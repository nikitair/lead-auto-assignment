from logging_config import logger as logging
import pretty_errors
import json
from .m_connector import mysql_connector

# logging.basicConfig(level=logging.INFO,
#                     format='%(asctime)s - %(levelname)s - %(message)s')


@mysql_connector
def get_realtors_in_polygon(connector, city, province, postalcode):
    """
    The major Realtors in Polygons searching function.

    * Current Logic:
    1. PostalCode provided -> searching by PostalCode
    2. Found realtors by PostalCode -> returning found realtors
    3. NO realtors found by PostalCode -> searching by City + Province (if provided)

    4. If only Province is provided -> NO search executed
    """

    QUERY = """
        SELECT DISTINCT
            tbl_zipcodes.City AS "City",
            CONCAT(tbl_customers.firstname, ' ', tbl_customers.lastname) AS `Name`,
            tbl_customers.email AS "Email"
        FROM
            tbl_market_leader_postal_codes AS res1
        LEFT JOIN tbl_zipcodes ON res1.postal_code_id = tbl_zipcodes.id
        LEFT JOIN tbl_customers ON res1.customer_id = tbl_customers.id
        WHERE
            (
                tbl_zipcodes.id IN (
                    SELECT
                        id
                    FROM
                        tbl_zipcodes
                    WHERE 
                        PostalCode = %s
                )
            OR (
                tbl_zipcodes.id IN (
                    SELECT
                        id
                    FROM
                        tbl_zipcodes
                    WHERE 
                        City = %s
                        AND (%s IS NULL OR Province = %s OR Province = "")
                )
            )
    """

    logging.info(f"{get_realtors_in_polygon.__name__} -- SELECTING REALTORS IN POLYGON")
    connector.execute(QUERY, (postalcode, city, province, province))  # Repeat province for the second %s

    data = connector.fetchall()
    logging.info(f"{get_realtors_in_polygon.__name__} -- SQL RESPONSE - {data}")
    
    return data


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
    logging.debug("TABLE tbl_customers CREATED")
    curr.close()


@mysql_connector
def get_all_customers(connector):
    curr = connector.cursor()
    curr.execute(
        "SELECT * FROM main.tbl_customers"
    )
    logging.debug("SELECTING DATA FROM tbl_customers")
    data = curr.fetchall()
    logging.debug(data)
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
    logging.debug(f"INSERTED TO tbl_customers {insert_payload[0]}")
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
    logging.debug("TABLE tbl_zipcodes CREATED")
    curr.close()


@mysql_connector
def get_all_zipcodes(connector):
    curr = connector.cursor()
    curr.execute(
        "SELECT * FROM main.tbl_zipcodes"
    )
    logging.debug("SELECTING DATA FROM tbl_zipcodes")
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
    logging.debug(f"INSERTED TO tbl_zipcodes {insert_payload[0]}")
    curr.close()


@mysql_connector
def create_table_tbl_market_leader_postal_codes(connector):
    curr = connector.cursor()
    curr.execute(
        """
        CREATE TABLE IF NOT EXISTS main.tbl_market_leader_postal_codes (
            customer_id INT PRIMARY KEY NOT NULL,
            postal_code_id INT NOT NULL
        );
        """
    )
    logging.debug("TABLE tbl_market_leader_postal_codes CREATED")
    curr.close()


@mysql_connector
def get_all_market_leader_postal_codes(connector):
    curr = connector.cursor()
    curr.execute(
        "SELECT * FROM main.tbl_market_leader_postal_codes LIMIT 10"
    )
    logging.debug("SELECTING DATA FROM tbl_market_leader_postal_codes")
    data = curr.fetchall()
    logging.debug(data)
    curr.close()
    return data


@mysql_connector
def add_market_leader_postal_code(connector, insert_payload: tuple):
    curr = connector.cursor()
    curr.executemany(
        """INSERT INTO main.tbl_market_leader_postal_codes
                (customer_id, postal_code_id)
            VALUES
                ( %s, %s)
    """,
        insert_payload
    )
    connector.commit()
    logging.debug(
        f"INSERTED TO market_leader_postal_codes {insert_payload[0]}")
    curr.close()


# LEGACY
# @mysql_connector
# def get_realtors_in_polygon(connector, city, province, postalcode):
#     curr = connector.cursor()

#     query_payload = [province]
#     query = """
#         SELECT DISTINCT
#             tbl_zipcodes.City AS "City",
#             CONCAT(tbl_customers.firstname, ' ', tbl_customers.lastname) AS `Name`,
#             tbl_customers.email AS "Email"
#         FROM
#             tbl_market_leader_postal_codes AS res1
#         LEFT JOIN tbl_zipcodes ON res1.postal_code_id = tbl_zipcodes.id
#         LEFT JOIN tbl_customers ON res1.customer_id = tbl_customers.id
#         WHERE
#         tbl_zipcodes.id IN (
#             SELECT
#                 id
#             FROM
#                 tbl_zipcodes
#         """

#     if postalcode:
#         query += " WHERE PostalCode = %s"
#         query_payload = [postalcode]
#         logging.info("SELECTING REALTORS IN POLYGON BY POSTALCODE")
#         query += " )"
#         curr.execute(query, tuple(query_payload))

#         data = curr.fetchall()
#         logging.info(f"{get_realtors_in_polygon.__name__} -- SQL RESPONSE BY POSTAL CODE - {data}")

#         if len(data) > 0:
#             curr.close()
#             return data
#         else:
#             logging.debug("1")
#             query = """
#                 SELECT DISTINCT
#                     tbl_zipcodes.City AS "City",
#                     CONCAT(tbl_customers.firstname, ' ', tbl_customers.lastname) AS `Name`,
#                     tbl_customers.email AS "Email"
#                 FROM
#                     tbl_market_leader_postal_codes AS res1
#                 LEFT JOIN tbl_zipcodes ON res1.postal_code_id = tbl_zipcodes.id
#                 LEFT JOIN tbl_customers ON res1.customer_id = tbl_customers.id
#                 WHERE
#                 tbl_zipcodes.id IN (
#                     SELECT
#                         id
#                     FROM
#                         tbl_zipcodes
#                 """
#             query_payload = [province]
#             query += " WHERE Province = %s"
#             if city:
#                 query += " AND City = %s"
#                 query_payload.append(city)
#             logging.info("SELECTING REALTORS IN POLYGON BY CITY/PROVINCE")
#             query += " )"
#             curr.execute(query, tuple(query_payload))

#             data = curr.fetchall()
#             logging.info(f"{get_realtors_in_polygon.__name__} -- SQL RESPONSE BY CITY/PROVINCE - {data}")
#             curr.close()
#             return data

#     else:
#         query_payload = [province]
#         query += " WHERE Province = %s"
#         if city:
#             query += " AND City = %s"
#             query_payload.append(city)
#         logging.info("SELECTING REALTORS IN POLYGON BY CITY/PROVINCE")
#         query += " )"
#         curr.execute(query, tuple(query_payload))

#         data = curr.fetchall()
#         logging.info(f"{get_realtors_in_polygon.__name__} -- SQL RESPONSE BY CITY/PROVINCE - {data}")
#         curr.close()
#         return data


if __name__ == "__main__":
    ...
