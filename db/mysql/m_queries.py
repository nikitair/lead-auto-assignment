import json
import re

from .logging_config import logger as logging

from .m_connector import mysql_connector

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
        logging.info(f"get_realtors_in_polygon -- SELECTING REALTORS IN POLYGON BY POSTALCODE")

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
        
        curr.execute(query, (postalcode,))

        data = curr.fetchall()
        logging.info(f"get_realtors_in_polygon -- SQL RESPONSE - {data}")
        
        return data
    
    else:
        logging.info(f"get_realtors_in_polygon -- SELECTING REALTORS IN POLYGON BY CITY/PROVINCE")

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
        curr.execute(query, tuple(query_payload))

        data = curr.fetchall()
        logging.info(f"get_realtors_in_polygon -- SQL RESPONSE - {data}")

        return data


@mysql_connector
def get_buyer_name(connector, buyer_email: str):
    logging.info(f"get_buyer_name -- SELECTING BUYER NAME BY EMAIL")
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
    logging.info(f"get_buyer_name -- SQL RESPONSE - {data}")

    return data[-1][-1] if data else None


@mysql_connector
def get_top_priority_realtors(connector, realtors: list):
    logging.info(f"get_top_priority_realtors -- SELECTING TOP PRIORITY REALTOR")

    result = []

    if not realtors:
        return result

    query = f"""
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
                        email IN ({", ".join(['%s' for _ in realtors])})
                )
            AND 
                email IN ({", ".join(['%s' for _ in realtors])})
            ORDER BY
                team_member_priority_for_lead_assign DESC;
        """

    curr = connector.cursor()
    curr.execute(query, realtors * 2)
    data = curr.fetchall()
    logging.info(f"get_top_priority_realtors -- SQL RESPONSE - {data}")

    priority_score = 0

    # prepare_result
    if data:
        result = [item[0] for item in data]
        priority_score = data[0][1]

    else:
        result = realtors
    return result, priority_score


@mysql_connector
def get_realtors_nationality(connector, realtors: list):
    logging.info(f"get_realtors_nationality -- SELECTING REALTOR NATIONALITY")

    result = [{item: None} for item in realtors]

    if not realtors:
        return result

    query = f"""
            SELECT
                email,
                team_member_preferred_nationalities as nationality
            FROM
                tbl_customers
            WHERE 
                email IN ({', '.join(['%s'] * len(realtors))})
            OR
                email IN ({', '.join(['%s'] * len(realtors))})
            ORDER BY
                tbl_customers.id DESC;
        """

    curr = connector.cursor()
    curr.execute(query, realtors + realtors)
    data = curr.fetchall()
    logging.info(f"get_realtors_nationality -- SQL RESPONSE - {data}")

    if data:

        nationality_descriptor = {
            "indian": "IN",
            "chinese": "CN"
        }

        result = []

        for item in data:

            if type(item[1]) == set:
                nationality = list(item[1])[0]
            else:
                nationality = item[1]

            result.append({item[0]: nationality_descriptor.get(nationality)})

    return result


# @mysql_connector
# def get_listing_category(connector, listing_mls: str):
#     logging.info(f"get_listing_category -- SELECTING LISTING CATEGORY - {listing_mls}")

#     query = """
#         SELECT
#             compiled_category_name AS category
#         FROM
#             tbl_advertisement
#         WHERE
#             DDF_ID = %s
#         LIMIT
#             1
#         """
#     curr = connector.cursor()
#     curr.execute(query, (listing_mls,))

#     data = curr.fetchall()
#     logging.info(f"get_listing_category -- SQL RESPONSE - {data}")

#     return data[0][-1] if data else None




@mysql_connector
def get_realtors_category(connector, realtors: list) -> list:
    logging.info(f"get_realtors_category -- SELECTING REALTORS CATEGORIES - {realtors}")
    query = f"""
                SELECT
                    email,
                    team_member_preferred_categories AS category
                FROM
                    tbl_customers
                WHERE
                    email IN ({', '.join(['%s'] * len(realtors))})
            """
    curr = connector.cursor()
    curr.execute(query, realtors)

    data = curr.fetchall()
    logging.info(f"get_realtors_category -- SQL RESPONSE - {data}")

    res = []
    if data:
        for element in data:
            res.append({"email": element[0], "category": element[1]})

    return res



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

    # print(get_realtors_nationality(realtors=['jack@fb4s.com', 'harman@fb4s.com', 'manoj@fb4s.com']))

    # print(get_listing_category(listing_mls="R2680048"))
    print(get_realtors_category(realtors=('drew@fb4s.com', 'manoj@fb4s.com')))
    # print(get_realtors_category(realtors=('dfgdfgdfg',)))
