import logging
from p_connector import postgres_connector

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')


@postgres_connector
def create_schema_statistics(connector):
    curr = connector.cursor()
    curr.execute(
        "CREATE SCHEMA IF NOT EXISTS statistics;"
    )
    logging.info("SCHEMA statistics CREATED")
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
    logging.info("TABLE statistics.market_leader_add_cities CREATED")
    curr.close()


@postgres_connector
def get_additional_cities(connector):
    curr = connector.cursor()
    curr.execute(
        "SELECT * FROM statistics.market_leader_add_cities"
    )
    logging.info("SELECTING DATA FROM statistics.market_leader_add_cities")
    data = curr.fetchall()
    logging.info(data)
    curr.close()
    return data


@postgres_connector
def add_additional_city(connector, insert_payload: tuple):
    curr = connector.cursor()

    # 1 - serching for same City AND Province combination
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
        logging.info("INSERTED TO statistics.market_leader_add_cities")

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
        logging.info("UPDATED statistics.market_leader_add_cities")
    curr.close()




if __name__ == "__main__":
    # create_schema_statistics()
    # create_table_additional_cities()
    add_additional_city(
        insert_payload=("Toronto", "Ontario", "1@mail.com")
    )
    get_additional_cities()
