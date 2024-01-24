from random import randint
from logging_config import logger as logging
import pretty_errors
from db.postgres import p_queries as postgres
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


def get_nationality(name: str):

    logging.info(f"{get_nationality} -- EVALUATING BUYER NATIONALITY")

    response = requests.get(
        f"https://api.nationalize.io/?name={name}"
    )

    status = response.status_code
    data = response.json()


    if status == 200 and type(data.get("country")) == list and len(data["country"]) > 0:
        nationality_array = [country["country_id"] for country in data["country"]]

    logging.info(f"{get_nationality} -- NATIONALITY API RESPONSE - {data}")
    
    if "IN" in nationality_array:
        return "IN"
    elif "CN" in nationality_array:
        return "CN"
    else:
        return None



def get_realtor_by_round_robin(realtors: list, buyer_name: str):
    """
    return realtor to assign according to the round-robin logic
    """
    assigned_realtor = None
    if type(realtors) == list and len(realtors) > 0:

        # PREMIUM realtors
        if "manoj@movewithmanoj.ca" in realtors:
            return "manoj@fb4s.com"
        elif "manoj@fb4s.com" in realtors:
            return "manoj@fb4s.com"
        
        realtors_nation_dict = {
            "CH": "jack@fb4s.com",
            "IN": "harman@fb4s.com"
        }
        
        buyer_nationality = get_nationality(buyer_name)
        
        if get_nationality(buyer_name) and ("jack@fb4s.com" in realtors or "harman@fb4s.com" in realtors):
            return realtors_nation_dict[buyer_nationality]

        try:
            assigned_realtor = postgres.get_realtor_to_assign(realtors)
            logging.info(
                f"{get_realtor_by_round_robin.__name__} -- ROUND-ROBIN ASSIGNED REALTOR -- {assigned_realtor}")
        except Exception as ex:
            logging.error(
                f"{get_realtor_by_round_robin.__name__} -- !!! ERROR OCCURRED -- {ex}")

        if assigned_realtor:
            assigned_realtor = assigned_realtor[-1][0]
        else:
            assigned_realtor = realtors[randint(
                0, len(realtors) - 1)]

    logging.info(
        f"{get_realtor_by_round_robin.__name__} -- RESULT ASSIGNED REALTOR -- {assigned_realtor}")
    return assigned_realtor


def get_pond_id(lead_province: str):
    url = "https://api.followupboss.com/v1/ponds?offset=0&limit=100"

    headers = {
        "accept": "application/json",
        "authorization": f"Basic {FUB_API_64}"
    }

    response = requests.get(url, headers=headers)
    data = response.json()
    logging.info(f"{get_pond_id.__name__} -- FUB RESPONSE -- {pprint.pformat(data)}")
    pond_id = 3

    if type(data) == dict and data.get("ponds"):
        for pond in data.get("ponds"):
            if f"{lead_province.title()} (Out of Polygon)" == pond["name"]:  
                pond_id = pond["id"]
                break
    logging.info(f"{get_pond_id.__name__} -- POND ID -- {pond_id}")
    return pond_id



if __name__ == "__main__":
    # get_pond_id("Manitoba")
    pprint.pprint(get_nationality("John"))