from random import randint
from logging_config import logger as logging
import pretty_errors
from db.postgres import p_queries as postgres
from db.mysql import m_queries as mysql
import requests
import os
import pprint
from dotenv import load_dotenv

load_dotenv()

FUB_API_64 = os.getenv("FUB_API_64")


def prepare_postalcode(postalcode: str):
    """
    formats formats code into desired format -- A1A1A1 -> A1A 1A1 
    """
    res = ''
    if type(postalcode) == str:
        if len(postalcode) > 0:
            if len(postalcode) == 6:
                res = f"{postalcode[0:3]} {postalcode[3:6]}"
            else:
                res = postalcode
    return res.upper()


def get_not_excluded_realtors(city: str, province: str, email_array: list) -> list:
    """
    returns realtors who are not in excluded cities table
    """
    excluded_emails = []
    for email in email_array:
        excluded_email = postgres.get_excluded_cities_by_city_province_emails(
            city, province, email)
        if len(excluded_email) > 0:
            excluded_emails.append(excluded_email[0][3])
    logging.info(f"EXCLUDED REALTORS -- {excluded_emails}")
    not_excluded_emails = [
        email for email in email_array if email not in excluded_emails]
    return not_excluded_emails


def get_nationality(name: str, search_nations):

    logging.info(f"{get_nationality.__name__} -- EVALUATING BUYER NATIONALITY")

    if not name or len(search_nations) < 1:
        return None

    response = requests.get(
        f"https://api.nationalize.io/?name={name}"
    )

    status = response.status_code
    data = response.json()


    if status == 200 and type(data.get("country")) == list and len(data["country"]) > 0:
        nationality_array = [country["country_id"] for country in data["country"]]

    logging.info(f"{get_nationality.__name__} -- NATIONALITY API RESPONSE - {data}")
    
    for nation in nationality_array:
        if nation in search_nations:
            return nation
        
    return None


def get_realtor_to_assign(realtors: list, buyer_name: str):
    """
    return realtor to assign according to the round-robin logic
    """
    assigned_realtor = None
    if type(realtors) == list and len(realtors) > 0:

        # 1. Evaluating top priority realtors (exit point)
        logging.info(f"{get_realtor_to_assign.__name__} -- 1. TOP PRIORITY EVALUATION")
        realtors = mysql.get_top_priority_realtors(realtors)
        logging.info(f"{get_realtor_to_assign.__name__} -- REALTORS BY TOP PRIORITY EVALUATIONS - {realtors}")

        if realtors and len(realtors) == 1:
            return realtors[0]
        
        # 2. Nationality evaluation (exit point)
        logging.info(f"{get_realtor_to_assign.__name__} -- 2. NATIONALITY EVALUATION")
        
        realtors_nationalities = mysql.get_realtors_nationality(realtors)
        buyer_nationality = get_nationality(buyer_name, [list(nation.values())[0] for nation in realtors_nationalities if list(nation.values())[0]])

        national_realtors = [list(realtor.keys())[0] for realtor in realtors_nationalities if list(realtor.values())[0] == buyer_nationality]
        logging.info(f"{get_realtor_to_assign.__name__} -- REALTORS BY NATIONAL EVALUATIONS - {national_realtors}")

        if len(national_realtors) > 0:
            realtors = national_realtors


        # 3. Category evaluation
        # to be implemented
        logging.info(f"{get_realtor_to_assign.__name__} -- 3. CATEGORY EVALUATION")


        # 4. Round Robin
        logging.info(f"{get_realtor_to_assign.__name__} -- 4. ROUND ROBIN EVALUATION")
        try:
            assigned_realtor = postgres.get_realtor_to_assign(realtors)
            logging.info(
                f"{get_realtor_to_assign.__name__} -- ROUND-ROBIN ASSIGNED REALTOR -- {assigned_realtor}")
        except Exception as ex:
            logging.error(
                f"{get_realtor_to_assign.__name__} -- !!! ERROR OCCURRED -- {ex}")

        if assigned_realtor:
            assigned_realtor = assigned_realtor[-1][0]
        else:
            assigned_realtor = realtors[randint(0, len(realtors) - 1)]

    logging.info(
        f"{get_realtor_to_assign.__name__} -- RESULT ASSIGNED REALTOR -- {assigned_realtor}")
    return assigned_realtor


def get_pond_id(lead_province: str):

    pond_id = 3

    url = "https://api.followupboss.com/v1/ponds?offset=0&limit=100"

    headers = {
        "accept": "application/json",
        "authorization": f"Basic {FUB_API_64}"
    }

    response = requests.get(url, headers=headers)

    logging.info(f"{get_pond_id.__name__} -- FUB RESPONSE STATUS -- {response.status_code}")

    try:
        data = response.json()
        logging.info(f"{get_pond_id.__name__} -- FUB RESPONSE DATA -- {data}")

    except Exception as ex:
        logging.error(f"{get_pond_id.__name__} -- !!! ERROR OCCURRED - {ex}")
        return pond_id

    if type(data) == dict and data.get("ponds"):
        for pond in data.get("ponds"):
            if f"{lead_province.title()} (Out of Polygon)" == pond["name"]:  
                pond_id = pond["id"]
                break
    logging.info(f"{get_pond_id.__name__} -- POND ID -- {pond_id}")

    return pond_id



if __name__ == "__main__":
    # get_pond_id("Manitoba")
    # pprint.pprint(get_nationality(None))
    print(get_nationality("Nikita"))