from database import Base
from sqlalchemy import String, DateTime, BIGINT
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql import text
import datetime


class SpimexTradingResults(Base):
    __tablename__ = 'spimex_trading_results'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    exchange_product_id: Mapped[str] = mapped_column(String(11))
    exchange_product_name: Mapped[str] = mapped_column(String(500), nullable=False)
    oil_id: Mapped[str] = mapped_column(String(4))
    delivery_basis_id: Mapped[str] = mapped_column(String(3))
    delivery_basis_name: Mapped[str] = mapped_column(String(300), nullable=False)
    delivery_type_id: Mapped[str] = mapped_column(String(1))
    volume: Mapped[int]
    total: Mapped[int] = mapped_column(BIGINT)
    count: Mapped[int]
    date: Mapped[datetime.datetime] = mapped_column(DateTime)
    created_on: Mapped[datetime.datetime] = mapped_column(server_default=text("TIMEZONE('utc', now())"))
    updated_on: Mapped[datetime.datetime] = mapped_column(
        server_default=text("TIMEZONE('utc', now())"),
        onupdate=datetime.datetime.utcnow,
    )
