from database import parquet_db
from datetime import datetime, timedelta
from models import Transaction, FractionedTransaction
import pandas as pd
from sqlalchemy import func


def extract(start_date_str, end_date_str):
    file_path = "data/sample_data_0007_part_00.parquet"
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
            transaction_date >= '{start_date_str}' 
        AND 
            transaction_date < '{end_date_str}'
    """
    result = parquet_db.execute(query).fetchall()
    return result


def transform(extracted_data):
    transactions_records = []
    for row in extracted_data:
        record = {
            "_id": row[0],
            "merchant_id": row[1],
            "subsidiary": row[2],
            "transaction_date": row[3],
            "account_number": row[4],
            "user_id": row[5],
            "transaction_amount": row[6],
            "transaction_type": row[7],
        }
        transactions_records.append(record)

    fractioned_transactions_records = []

    fractioned_transaction_label = FractionedTransaction.query.with_entities(
        func.max(FractionedTransaction.transaction_label)
    ).scalar()

    if not fractioned_transaction_label:
        fractioned_transaction_label = 0

    for name, group_df in pd.DataFrame(transactions_records).groupby(
        ["user_id", "subsidiary", pd.Grouper(key="transaction_date", freq="D")]
    ):
        if len(group_df) > 1:
            fractioned_transaction_label += 1
            total_amount = group_df["transaction_amount"].sum()
            for row in group_df.itertuples():
                record = {
                    "transaction_id": row._1,
                    "transaction_counts": len(group_df),
                    "transaction_label": fractioned_transaction_label,
                    "transaction_total_amount": total_amount,
                }
                fractioned_transactions_records.append(record)

    return transactions_records, fractioned_transactions_records


def load(app, db, transaction_records, fractioned_transaction_records):
    try:
        db.session.bulk_insert_mappings(Transaction, transaction_records)
        db.session.bulk_insert_mappings(
            FractionedTransaction, fractioned_transaction_records
        )
        db.session.commit()
    except Exception as e:
        raise ValueError(e)


def etl_new_data(app, db):
    current_date = datetime.now().date()
    # get the last date from transactions table
    last_transaction = Transaction.query.order_by(
        Transaction.transaction_date.desc()
    ).first()
    if last_transaction:
        last_transaction_date = last_transaction.transaction_date
    else:
        last_transaction_date = None

    if current_date > last_transaction_date.date():
        start_date = last_transaction_date + timedelta(days=1)
        end_date = start_date + timedelta(days=1)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
    else:
        return  # No new data to extract

    extracted_data = extract(start_date_str, end_date_str)

    transaction_records, fractioned_transactions_records = transform(extracted_data)

    load(app, db, transaction_records, fractioned_transactions_records)

    return
