from flask import Flask
from database import db
from models import Transaction, FractionedTransaction
import duckdb


def create_database():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///test.db"
    db.init_app(app)

    with app.app_context():
        db.create_all()
        print("Database and tables created.")


def load_initial_transactions():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///test.db"
    db.init_app(app)

    con = duckdb.connect(database=":memory:")
    file_path = "data/sample_data_0007_part_00.parquet"
    start_date = "2021-01-01"
    end_date = "2021-01-05"

    query = f"""
        SELECT 
            _id,
            merchant_id,
            subsidiary,
            transaction_date,
            account_number,
            user_id,
            transaction_amount,
            transaction_type
        FROM 
            read_parquet('{file_path}')
        WHERE
            transaction_date >= '{start_date}' 
        AND 
            transaction_date < '{end_date}'
    """

    result = con.execute(query).fetchall()
    # convert the result to a list of dictionaries
    transaction_records = [
        {
            "_id": row[0],
            "merchant_id": row[1],
            "subsidiary": row[2],
            "transaction_date": row[3],
            "account_number": row[4],
            "user_id": row[5],
            "transaction_amount": row[6],
            "transaction_type": row[7],
        }
        for row in result
    ]
    with app.app_context():
        db.session.bulk_insert_mappings(Transaction, transaction_records)
        db.session.commit()
        print(f"Inserted {len(transaction_records)} transactions into the database.")

    con.close()


def load_initial_fractioned_transactions():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///test.db"
    db.init_app(app)

    con = duckdb.connect(database=":memory:")
    file_path = "data/sample_data_0007_part_00.parquet"
    start_date = "2021-01-01"
    end_date = "2021-01-05"

    query_1 = f"""
        SELECT 
            _id,
            merchant_id,
            subsidiary,
            transaction_date,
            account_number,
            user_id,
            transaction_amount,
            transaction_type
        FROM 
            read_parquet('{file_path}')
        WHERE
            transaction_date >= '{start_date}' 
        AND 
            transaction_date < '{end_date}'
    """

    query_2 = f"""
        WITH transactions AS(
            {query_1}
        ),
        frac_transactions AS (
            SELECT         
                user_id,
                subsidiary,
                DATE_TRUNC('day', transaction_date) as transaction_day,
                COUNT(*) AS transaction_counts,
                SUM(transaction_amount) as transaction_total_amount,
                ROW_NUMBER() OVER () AS label
            FROM 
                transactions
            GROUP BY 
                user_id, 
                subsidiary, 
                DATE_TRUNC('day', transaction_date)
            HAVING
                COUNT(*) > 1
        )
        SELECT 
            t._id AS transaction_id,
            frac.transaction_counts as transaction_counts,
            frac.label AS transaction_label, 
            frac.transaction_total_amount as transaction_total_amount
        FROM 
            transactions AS t
        INNER JOIN 
            frac_transactions AS frac
        ON 
            t.user_id = frac.user_id
            AND t.subsidiary = frac.subsidiary
            AND DATE_TRUNC('day', t.transaction_date) = frac.transaction_day 

    """

    result = con.execute(query_2).fetchall()
    # convert the result to a list of dictionaries
    frac_transaction_records = [
        {
            "transaction_id": row[0],
            "transaction_counts": row[1],
            "transaction_label": row[2],
            "transaction_total_amount": row[3],
        }
        for row in result
    ]
    with app.app_context():
        # Logic to create fractioned transactions from transaction_records
        db.session.bulk_insert_mappings(FractionedTransaction, frac_transaction_records)
        db.session.commit()
        print(
            f"Inserted {len(frac_transaction_records)} transactions into the database."
        )

    con.close()


if __name__ == "__main__":
    create_database()
    load_initial_transactions()
    load_initial_fractioned_transactions()
