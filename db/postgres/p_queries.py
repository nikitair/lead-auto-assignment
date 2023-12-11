import logging
import pretty_errors
from .p_connector import postgres_connector

# logging.basicConfig(level=logging.DEBUG,
#                     format='%(asctime)s - %(levelname)s - %(message)s')


@postgres_connector
def create_schema_statistics(connector):
    curr = connector.cursor()
    curr.execute(
        "CREATE SCHEMA IF NOT EXISTS statistics;"
    )
    logging.debug("SCHEMA statistics CREATED")
    curr.close()


@postgres_connector
def create_table_additional_cities(connector):
    curr = connector.cursor()
    curr.execute(
        """CREATE TABLE statistics.market_leader_add_cities (
                id SERIAL PRIMARY KEY NOT NULL,
                city VARCHAR(255) NOT NULL,
                province VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                created_at timestamp default current_timestamp NOT NULL
            );

        """
    )
    logging.debug("TABLE statistics.market_leader_add_cities CREATED")
    curr.close()


@postgres_connector
def get_all_additional_cities(connector):
    curr = connector.cursor()
    curr.execute(
        "SELECT * FROM statistics.market_leader_add_cities"
    )
    logging.debug("SELECTING DATA FROM statistics.market_leader_add_cities")
    data = curr.fetchall()
    logging.debug(data)
    curr.close()
    return data


@postgres_connector
def add_additional_city(connector, insert_payload: tuple):
    curr = connector.cursor()

    # searching for the same City AND Province combination
    curr.execute("SELECT * FROM statistics.market_leader_add_cities WHERE city = %s AND province = %s",
                 (insert_payload[0], insert_payload[1]))
    duplicates = curr.fetchall()

    if len(duplicates) < 1:
        curr.execute(
            """INSERT INTO statistics.market_leader_add_cities 
                    (city, province, email, created_at)
                VALUES
                    ( %s, %s, %s, NOW());
            """,
            insert_payload
        )
        connector.commit()
        logging.debug("INSERTED TO statistics.market_leader_add_cities")

    else:
        curr.execute(
            """UPDATE statistics.market_leader_add_cities
                SET 
                    email = %s,
                    created_at = NOW()
                WHERE 
                    city = %s AND province = %s;
            """,
            (insert_payload[2], insert_payload[0], insert_payload[1])
        )
        connector.commit()
        logging.debug("UPDATED statistics.market_leader_add_cities")
    curr.close()


@postgres_connector
def create_table_excluded_cities(connector):
    curr = connector.cursor()
    curr.execute(
        """CREATE TABLE statistics.market_leader_excl_cities (
                id SERIAL PRIMARY KEY NOT NULL,
                city VARCHAR(255) NOT NULL,
                province VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                created_at timestamp default current_timestamp NOT NULL
            );

        """
    )
    logging.debug("TABLE statistics.market_leader_excl_cities CREATED")
    curr.close()


@postgres_connector
def get_all_excluded_cities(connector):
    curr = connector.cursor()
    curr.execute(
        "SELECT * FROM statistics.market_leader_excl_cities"
    )
    logging.debug("SELECTING DATA FROM statistics.market_leader_excl_cities")
    data = curr.fetchall()
    logging.debug(data)
    curr.close()
    return data


@postgres_connector
def add_excluded_city(connector, insert_payload: tuple):

    # check if this email and city / province combination does not exist
    curr = connector.cursor()
    curr.execute(
        """SELECT * FROM statistics.market_leader_excl_cities 
        WHERE 
            city = %s
        AND
            province = %s
        AND
            email = %s;
        """,
        insert_payload
    )
    duplicates = curr.fetchall()
    if len(duplicates) < 1:
        curr = connector.cursor()
        curr.execute(
            """INSERT INTO statistics.market_leader_excl_cities
                    (city, province, email, created_at)
                VALUES
                    ( %s, %s, %s, NOW());
        """,
            insert_payload
        )
        connector.commit()
        logging.debug("INSERTED TO statistics.market_leader_excl_cities")
    curr.close()


@postgres_connector
def get_additional_cities_by_city_province(connector, city: str, province: str):
    curr = connector.cursor()
    curr.execute(
        """
        SELECT * FROM statistics.market_leader_add_cities
        WHERE city = %s
        AND province = %s
        """,
        (city, province)
    )
    logging.debug("SELECTING DATA FROM statistics.market_leader_add_cities")
    data = curr.fetchall()
    logging.debug(data)
    curr.close()
    return data


@postgres_connector
def get_excluded_cities_by_city_province_emails(connector, city, province, email):
    curr = connector.cursor()
    curr.execute(
        """
        SELECT * FROM statistics.market_leader_excl_cities
        WHERE
            city = %s
        AND
            province = %s
        AND
            email = %s
        LIMIT 1
        """,
        (city, province, email)
    )
    logging.debug("SELECTING DATA FROM statistics.market_leader_excl_cities")
    data = curr.fetchall()
    logging.debug(data)
    curr.close()
    return data


if __name__ == "__main__":
    add_additional_city(
        insert_payload=("Fergus", "Ontario", "nikita.stoliarov+1@actse.ltd")
    )
    get_all_additional_cities()
    # add_excluded_city(insert_payload=(
    #     "Hanover", "Ontario", "nikita.stoliarov+1@actse.ltd"))
    # get_all_excluded_cities()
