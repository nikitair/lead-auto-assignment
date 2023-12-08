from flask import Flask, request, jsonify
import pretty_errors
import pprint
import logging
from main import main

app = Flask(__name__)

@app.route('/lead_auto_assignment', methods=['POST'])
def receive_json():
    try:
        payload = request.get_json()
        logging.info(f"PAYLOAD RECEIVED -- {pprint.pformat(payload)}\n")

        postalcode = payload["listing_zip"]
        listing_province = payload["listing_province"]
        buyer_city = payload["buyer_city"]
        buyer_province = payload["buyer_province"]

        result = main(postalcode, listing_province, buyer_city, buyer_province)
        return jsonify(result), 200
    
    except Exception as e:
        error_message = {"status": "fail", "error": "Bad request", "details": str(e)}
        return jsonify(error_message), 400

if __name__ == '__main__':
    app.run(debug=False)
