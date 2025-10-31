import os
from database import parquet_db
from datetime import datetime, timedelta
from models import Transaction, FractionedTransaction
import pandas as pd
from sqlalchemy import func, text


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


def load(db, transaction_records, fractioned_transaction_records):
    try:
        db.session.bulk_insert_mappings(Transaction, transaction_records)
        db.session.bulk_insert_mappings(
            FractionedTransaction, fractioned_transaction_records
        )
        db.session.commit()

        update_merchant_grouped_table(db)
        update_account_grouped_table(db)

    except Exception as e:
        raise ValueError(e)


def update_merchant_grouped_table(db):
    query = f"""
    WITH
    ft_src AS (
    SELECT * FROM fractioned_transactions
    ),
    full_table AS (
    SELECT *
    FROM transactions AS txn
    LEFT JOIN ft_src AS ft
        ON txn._id = ft.transaction_id
    ),
    -- keep only non-null amounts for percentile rank
    amounts AS (
    SELECT merchant_id, subsidiary, transaction_amount
    FROM full_table
    WHERE transaction_amount IS NOT NULL
    ),
    ranked AS (
    SELECT
        merchant_id,
        subsidiary,
        transaction_amount,
        ROW_NUMBER() OVER (
        PARTITION BY merchant_id, subsidiary
        ORDER BY transaction_amount
        ) AS rn,
        COUNT(*) OVER (
        PARTITION BY merchant_id, subsidiary
        ) AS n
    FROM amounts
    ),
    -- 90th percentile via rank = ceil(0.9 * n)
    p90 AS (
    SELECT
        merchant_id,
        subsidiary,
        transaction_amount AS percentile_90
    FROM ranked
    WHERE rn = CAST((n - 1) * 0.9 AS INT) + 1
    ),
    first_subsidiary_agg AS (
    SELECT
        ft.merchant_id,
        ft.subsidiary,
        MAX(ft.transaction_amount) AS max_transaction_amount,
        p.percentile_90,
        AVG(ft.transaction_amount) AS avg_amount,
        -- population stddev: sqrt(E[x^2] - (E[x])^2)
        CASE
        WHEN COUNT(ft.transaction_amount) > 0 THEN
            sqrt(AVG(ft.transaction_amount * ft.transaction_amount)
                - AVG(ft.transaction_amount) * AVG(ft.transaction_amount))
        ELSE NULL
        END AS stddev_amount,
        SUM(ft.transaction_amount) AS total_transactions_amount,
        COUNT(DISTINCT ft.transaction_label) AS fractioned_transaction_counts
    FROM full_table AS ft
    LEFT JOIN p90 AS p
        ON p.merchant_id = ft.merchant_id
    AND p.subsidiary  = ft.subsidiary
    GROUP BY ft.merchant_id, ft.subsidiary
    ),
    second_subsidiary_agg AS (
    SELECT
        merchant_id,
        subsidiary,
        transaction_label,
        MAX(transaction_amount) AS max_txn_in_fraction
    FROM full_table
    WHERE transaction_label IS NOT NULL
    GROUP BY merchant_id, subsidiary, transaction_label
    ),
    final_aggregation AS (
    SELECT
        fsa.merchant_id,
        fsa.subsidiary,
        SUM(CASE
            WHEN fsa.max_transaction_amount = ssa.max_txn_in_fraction
            THEN 1 ELSE 0
            END) AS matching_fractioned_max_amts
    FROM first_subsidiary_agg AS fsa
    LEFT JOIN second_subsidiary_agg AS ssa
        ON fsa.merchant_id = ssa.merchant_id
    AND fsa.subsidiary  = ssa.subsidiary
    GROUP BY fsa.merchant_id, fsa.subsidiary
    )
    SELECT
    fsa.*,
    fa.matching_fractioned_max_amts,
    (1.0 * fa.matching_fractioned_max_amts) / NULLIF(fsa.fractioned_transaction_counts, 0)
        AS fractioned_max_amt_match_ratio
    FROM first_subsidiary_agg AS fsa
    JOIN final_aggregation AS fa
    ON fsa.merchant_id = fa.merchant_id
    AND fsa.subsidiary  = fa.subsidiary;
    """

    result_df = pd.read_sql_query(query, db.engine)
    save_dir = "data/processed/"
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    result_df.to_csv(f"{save_dir}/merchant_grouped_transactions.csv", index=False)


def update_account_grouped_table(db):
    query = f"""
    WITH 
    ft_src AS (
    SELECT * FROM fractioned_transactions
    ),
    full_table AS (
    SELECT *
    FROM transactions AS txn
    LEFT JOIN ft_src AS ft
        ON txn._id = ft.transaction_id
    ),
    first_account_agg AS (
    SELECT
        account_number,
        MAX(transaction_amount)                                   AS max_transaction_amount,
        AVG(transaction_amount)                                   AS avg_amount,
        /* population stddev = sqrt(E[x^2] - (E[x])^2) */
        CASE 
        WHEN COUNT(transaction_amount) > 0 THEN
            sqrt(AVG(transaction_amount * transaction_amount)
                - AVG(transaction_amount) * AVG(transaction_amount))
        ELSE NULL
        END                                                       AS stddev_amount,
        SUM(transaction_amount)                                   AS total_transactions_amount,
        COUNT(DISTINCT transaction_label)                         AS fractioned_transaction_counts
    FROM full_table
    GROUP BY account_number
    ), 
    second_account_agg AS (
    SELECT
        account_number,
        transaction_label,
        MAX(transaction_amount) AS max_txn_in_fraction
    FROM full_table
    WHERE transaction_label IS NOT NULL
    GROUP BY account_number, transaction_label
    ),
    final_aggregation AS (
    SELECT
        faa.account_number,
        SUM(CASE
            WHEN faa.max_transaction_amount = saa.max_txn_in_fraction THEN 1
            ELSE 0
            END) AS matching_fractioned_max_amts
    FROM first_account_agg AS faa
    LEFT JOIN second_account_agg AS saa
        ON faa.account_number = saa.account_number
    GROUP BY faa.account_number
    )
    SELECT 
    faa.*,
    fa.matching_fractioned_max_amts,
    (1.0 * fa.matching_fractioned_max_amts) 
        / NULLIF(faa.fractioned_transaction_counts, 0) AS fractioned_max_amt_match_ratio
    FROM first_account_agg AS faa
    JOIN final_aggregation AS fa
    ON faa.account_number = fa.account_number;

    """
    result_df = pd.read_sql_query(query, db.engine)
    save_dir = "data/processed/"
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    result_df.to_csv(f"{save_dir}/account_grouped_transactions.csv", index=False)


def etl_new_data(db):
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

    load(db, transaction_records, fractioned_transactions_records)

    return
