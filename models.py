from database import db
from datetime import datetime
from sqlalchemy import DECIMAL


class Transaction(db.Model):
    __tablename__ = "transactions"

    _id = db.Column(db.String(100), nullable=False, primary_key=True)
    merchant_id = db.Column(db.String(50), nullable=False)
    subsidiary = db.Column(db.String(50), nullable=True)
    transaction_date = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    account_number = db.Column(db.String(100), nullable=False)
    user_id = db.Column(db.String(100), nullable=False)
    transaction_amount = db.Column(DECIMAL(24, 8), nullable=False)
    transaction_type = db.Column(db.String(50), nullable=False)

    def to_dict(self):
        return {
            "_id": self.id,
            "transaction_date": self.transaction_date,
            "user_id": self.user_id,
            "merchant_id": self.merchant_id,
            "transaction_amount": self.transaction_amount,
            "transaction_type": self.transaction_type,
        }


class FractionedTransaction(db.Model):
    __tablename__ = "fractioned_transactions"

    transaction_id = db.Column(
        db.String(100), db.ForeignKey("transactions._id"), primary_key=True
    )
    transaction_counts = db.Column(db.Integer, nullable=False)
    transaction_label = db.Column(db.Integer, nullable=False)
    transaction_total_amount = db.Column(DECIMAL(24, 8), nullable=False)

    def to_dict(self):
        return {
            "transaction_id": self.transaction_id,
            "transaction_counts": self.transaction_counts,
            "transaction_label": self.transaction_label,
            "transaction_total_amount": self.transaction_total_amount,
        }
