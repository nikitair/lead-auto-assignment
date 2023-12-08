import json
import logging
from utils import prepare_postalcode, get_not_excluded_realtors
from db.postgres import p_queries as postgres
from db.mysql import m_queries as mysql

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def main(mls: str, postalcode: str, listing_province: str, buyer_city: str, buyer_province: str, email: str) -> dict:
    response = {
        "realtor_1": 0,
        "realtor_emails": []
    }

    province = listing_province if listing_province not in (None, "") else buyer_province
    postalcode = prepare_postalcode(postalcode)

    # search in additional cities
    additional_cities = postgres.get_additional_cities_by_city_province(
        city=buyer_city, province=province)
    logging.info(f"ADDITIOANL CITIES -- {additional_cities}")

    if len(additional_cities) > 0:
        response["realtor_1"] = 1
        for city in additional_cities:
            response["realtor_emails"].append(city[3])
    else:
        # search in polygons
        realtors_in_polygon = [realtor[2] for realtor in mysql.get_realtors_in_polygon(province, postalcode)]
        logging.info(f"ALL REALTORS IN POLYGON -- {realtors_in_polygon}")

        if len(realtors_in_polygon) > 0:
            # search for realtors who are NOT excluded
            not_excluded_realtors = get_not_excluded_realtors(buyer_city, province, realtors_in_polygon)
            logging.info(f"NOT EXCLUDED REALTORS -- {not_excluded_realtors}")

            if len(not_excluded_realtors) > 0:
                response["realtor_1"] = 1
                for realtor in not_excluded_realtors:
                    response["realtor_emails"].append(realtor)


    logging.info(f"RESULT RESPONSE -- {response}")
    return response


if __name__ == "__main__":
    with open("demo_payloads.json", "r") as f:
        demo_payloads = json.load(f)
        payload = demo_payloads["intersects_one_polygon"]
        mls = payload["listing_mls"]
        postalcode = payload["listing_zip"]
        listing_province = payload["listing_province"]
        buyer_city = payload["buyer_city"]
        buyer_province = payload["buyer_province"]
        email = payload["buyer_email"]

    main(mls, postalcode, listing_province, buyer_city, buyer_province, email)
