from flask import Flask, request, jsonify
import pretty_errors
import pprint
import logging
from main import main

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def index():
    """
    entry endpoint for server running check 
    """
    try:
        payload = request.get_json()
        logging.info(f"PAYLOAD RECEIVED -- {pprint.pformat(payload)}")
        return jsonify({"status": "success", "message": "Hello World!", "payload": payload}), 200
    except Exception as e:
        error_message = {"status": "fail", "error": "Bad request", "details": str(e)}
        logging.error(f"\n!!! SERVER ERROR OCCURRED -- {str(e)}\n")
        return jsonify(error_message), 400


@app.route('/lead_auto_assignment', methods=['POST'])
def lead_auto_assignment():
    """
    lead auto assignment endpoint
    """
    try:
        # receiving lead payload
        payload = request.get_json()
        logging.info(f"PAYLOAD RECEIVED -- {pprint.pformat(payload)}\n")

        # extracting useful information from the payload
        postalcode = payload["listing_zip"]
        listing_province = payload["listing_province"]
        buyer_city = payload["buyer_city"]
        buyer_province = payload["buyer_province"]

        # executing lead auto assignment function; returning result
        result = main(postalcode, listing_province, buyer_city, buyer_province)
        return jsonify(result), 200
    
    except Exception as e:
        error_message = {"status": "fail", "error": "Bad request", "details": str(e)}
        logging.error(f"\n!!! SERVER ERROR OCCURRED -- {str(e)}\n")
        return jsonify(error_message), 400

if __name__ == '__main__':
    app.run(debug=False, port=8080)
