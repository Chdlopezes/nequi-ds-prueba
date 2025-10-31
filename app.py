from database import db, parquet_db
from flask import Flask, jsonify
from datetime import datetime, timedelta
from utils import etl_new_data

app = Flask(__name__)

app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///test.db"

db.init_app(app)


# create toy route
@app.route("/")
def home():
    return "Welcome to the Transaction API"


@app.route("/update_transactions", methods=["GET"])
def update_transactions():
    try:
        etl_new_data(app, db)
    except Exception as e:
        return jsonify({"error": e}), 500
    return jsonify({"state": "success"}), 200


if __name__ == "__main__":
    app.run(debug=True)
