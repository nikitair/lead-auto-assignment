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
    curr.close()


@postgres_connector
def create_table_additional_cities(connector):
    curr = connector.cursor()
    curr.execute(
        """CREATE TABLE IF NOT EXISTS statistics.market_leader_add_cities (
                id SERIAL PRIMARY KEY,
                city VARCHAR(255) NOT NULL,
                province VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                created_at timestamp default current_timestamp NOT NULL
            );
        """
        )
    curr.close()


if __name__ == "__main__":
    # create_schema_statistics()
    create_table_additional_cities()
