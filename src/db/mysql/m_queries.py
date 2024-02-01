from logging_config import logger as logging
import pretty_errors
import json
from .m_connector import mysql_connector
import re

# logging.basicConfig(level=logging.INFO,
#                     format='%(asctime)s - %(levelname)s - %(message)s')

def is_valid_postal_code(postal_code):
    """
    checks is PostalCode is valid
    """
    pattern = r"^[A-Za-z]\d[A-Za-z] \d[A-Za-z]\d$"
    return re.match(pattern, postal_code) is not None


@mysql_connector
def get_realtors_in_polygon(connector, city, province, postalcode):
    curr = connector.cursor()

    if postalcode and is_valid_postal_code(postalcode):

        query = """
            SELECT DISTINCT
                tbl_zipcodes.City AS "City",
                CONCAT(tbl_customers.firstname, ' ', tbl_customers.lastname) AS `Name`,
                tbl_customers.email AS "Email"
            FROM
                tbl_market_leader_postal_codes AS res1
            LEFT JOIN tbl_zipcodes ON res1.postal_code_id = tbl_zipcodes.id
            LEFT JOIN tbl_customers ON res1.customer_id = tbl_customers.id
            WHERE
            tbl_zipcodes.id IN (
                SELECT
                    id
                FROM
                    tbl_zipcodes
                WHERE 
                    PostalCode = %s
                )
        """
        
        logging.info(f"{get_realtors_in_polygon.__name__} -- SELECTING REALTORS IN POLYGON BY POSTALCODE")
        curr.execute(query, (postalcode,))

        data = curr.fetchall()
        logging.info(f"{get_realtors_in_polygon.__name__} -- SQL RESPONSE - {data}")
        
        return data
    
    else:
        query = """
        SELECT DISTINCT
            tbl_zipcodes.City AS "City",
            CONCAT(tbl_customers.firstname, ' ', tbl_customers.lastname) AS `Name`,
            tbl_customers.email AS "Email"
        FROM
            tbl_market_leader_postal_codes AS res1
        LEFT JOIN tbl_zipcodes ON res1.postal_code_id = tbl_zipcodes.id
        LEFT JOIN tbl_customers ON res1.customer_id = tbl_customers.id
        WHERE
        tbl_zipcodes.id IN (
            SELECT
                id
            FROM
                tbl_zipcodes
            WHERE 
                City = %s
        """
        query_payload = [city]
        if province:
            query_payload.append(province)
            query += " AND Province = %s"
        query += " )"
        logging.info(f"{get_realtors_in_polygon.__name__} -- SELECTING REALTORS IN POLYGON BY CITY/PROVINCE")
        curr.execute(query, tuple(query_payload))

        data = curr.fetchall()
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


@mysql_connector
def get_buyer_name(connector, buyer_email: str):
    logging.info(f"{get_buyer_name.__name__} -- SELECTING DATA IN tbl_customers")
    curr = connector.cursor()
    curr.execute(
        """
            SELECT
                firstname
            FROM
                tbl_customers
            WHERE
                email = %s
            LIMIT 1
        """,
        (buyer_email,)
    )

    data = curr.fetchall()
    logging.info(f"{get_buyer_name.__name__} -- SQL RESPONSE - {data}")

    return data[-1][-1] if data else None


@mysql_connector
def get_top_priority_realtors(connector, realtors: list):
    logging.info(f"{get_top_priority_realtors.__name__} -- SELECTING DATA IN tbl_customers")

    result = []

    if not realtors:
        return result
    
    query = """

            SELECT
                email,
                team_member_priority_for_lead_assign AS priority
            FROM
                tbl_customers
            WHERE
                team_member_priority_for_lead_assign = (
                    SELECT
                        MAX(team_member_priority_for_lead_assign) AS max_priority
                    FROM
                        tbl_customers
                    WHERE
                        email IN %s
            )
            AND 
                email IN %s
            ORDER BY
                team_member_priority_for_lead_assign DESC;
        """
    curr = connector.cursor()
    curr.execute(query, (realtors, realtors))
    data = curr.fetchall()
    logging.info(f"{get_top_priority_realtors.__name__} -- SQL RESPONSE - {data}")

    # prepare_result
    if data:
        result = [item[0] for item in data]
    else:
        result = realtors
    return result


@mysql_connector
def get_realtors_nationality(connector, realtors: list):
    logging.info(f"{get_realtors_nationality.__name__} -- SELECTING DATA IN tbl_customers")

    result = [{item: None} for item in realtors]

    if not realtors:
        return result
    
    query = """
            SELECT
                email,
                team_member_preferred_nationalities as nationality
            FROM
                tbl_customers
            WHERE 
                email IN %s
            OR
                email IN %s
            ORDER BY
                id DESC
        """
    curr = connector.cursor()
    curr.execute(query, (realtors, realtors))
    data = curr.fetchall()
    logging.info(f"{get_realtors_nationality.__name__} -- SQL RESPONSE - {data}")

    if data:

        nationality_descriptor = {
            "indian": "IN",
            "chinese": "CN"
        }

        result = []

        for item in data:
            result.append({item[0]: nationality_descriptor.get(item[1])})

    return result





if __name__ == "__main__":
    # print(is_valid_postal_code("A1A 1A1"))
    # print(get_buyer_name("hiba.shahbaz@gmail.com"))
    # print(get_buyer_name("test.com"))
    # print(get_realtors_in_polygon("Toronto", "Ontario", "A1A 1A1"))

    # print(get_top_priority_realtors(['jack@fb4s.com', 'drew@fb4s.com', 'omgil12@yahoo.com']))
    # print(get_top_priority_realtors(['jack@fb4s.com']))
    # print(get_top_priority_realtors(['a']))
    # print(get_top_priority_realtors(['soraia@fb4s.com', 'manoj@fb4s.com']))
    # print(get_top_priority_realtors(['soraia@fb4s.com', 'jack@fb4s.com']))
    # print(get_top_priority_realtors(['duncan@fb4s.com']))
    # print(get_top_priority_realtors(['duncan@fb4s.com', 'nikita@fb4s.com']))
    # print(get_top_priority_realtors(['soraia@fb4s.com', 'Manoj@MoveWithManoj.ca', 'manoj@fb4s.com']))
    # print(get_top_priority_realtors(['Manoj@MoveWithManoj.ca', 'manoj@fb4s.com']))

    print(get_realtors_nationality(realtors=['jack@fb4s.com', 'harman@fb4s.com', 'manoj@fb4s.com']))
