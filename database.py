# Connection to database
import duckdb
from flask_sqlalchemy import SQLAlchemy


db = SQLAlchemy()


parquet_db = duckdb.connect(database=":memory:")
