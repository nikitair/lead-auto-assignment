from logging_config import logger as logging
import pretty_errors
from .p_connector import postgres_connector


@postgres_connector
def get_additional_cities(connector, city: str, province: str):
    logging.info(f"get_additional_cities -- SELECTING ADDITIONAL CITY - {city} - {province}")

    curr = connector.cursor()
    curr.execute(
        """
        SELECT * FROM statistics.market_leader_add_cities
        WHERE city = %s
        AND province = %s
        """,
        (city, province)
    )

    data = curr.fetchall()

    logging.info(f"get_additional_cities -- SQL RESPONSE - {data}")
    curr.close()

    return data


@postgres_connector
def get_excluded_cities(connector, city, province, email):
    logging.info(f"get_excluded_cities -- SELECTING EXCLUDED CITY - {email} - {province}")

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

    data = curr.fetchall()

    logging.info(f"get_excluded_cities -- SQL RESPONSE - {data}")
    curr.close()

    return data


@postgres_connector
def get_realtor_by_round_robin(connector, realtor_emails):
    logging.info(f"get_realtor_by_round_robin -- SELECTING REALTOR TO ASSIGN BY ROUND ROBIN - {realtor_emails}")

    curr = connector.cursor()
    curr.execute(
        """
        SELECT
        email,
        latest_time
        FROM (
            SELECT
                realtor_email AS email,
                MAX(assign_time) AS latest_time
            FROM
                statistics.lead_auto_assignment
            WHERE
                realtor_email IN %s
            GROUP BY
                realtor_email
        ) AS res
        ORDER BY
            latest_time ASC
        LIMIT 1;
        """,
        (tuple(realtor_emails),)
    )
    data = curr.fetchall()

    logging.info(f"get_realtor_by_round_robin -- SQL RESPONSE - {data}")
    curr.close()
    
    return data


if __name__ == "__main__":
    ...
