# database/models.py
from sqlalchemy import Column, Integer, String, Enum, BigInteger, TIMESTAMP, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
import enum

Base = declarative_base()

class UserRole(enum.Enum):
    admin = "admin"
    user = "user"
    manager = "manager"

class MovementType(enum.Enum):
    prihod = "prihod"
    korrekt = "korrekt"
    postavka = "postavka"

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False)
    name = Column(String(255))
    role = Column(Enum(UserRole, name="user_role_enum"), nullable=False)  # Указываем имя
    password_hash = Column(String(255), nullable=False)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())

class Warehouse(Base):
    __tablename__ = "warehouses"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    is_active = Column(Boolean, default=True)

class Product(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True)
    article = Column(String(100), unique=True, nullable=False)
    name = Column(String(255), nullable=False)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    is_active = Column(Boolean, default=True)

class StockMovement(Base):
    __tablename__ = "stock_movements"
    id = Column(Integer, primary_key=True)
    warehouse_id = Column(Integer, ForeignKey("warehouses.id"))
    product_id = Column(Integer, ForeignKey("products.id"))  # Исправлено с warehouses на products
    qty = Column(Integer, nullable=False)
    type = Column(Enum(MovementType, name="movement_type_enum"), nullable=False)  # Указываем имя
    date = Column(TIMESTAMP, server_default=func.current_timestamp())
    doc_id = Column(Integer)
    user_id = Column(Integer, ForeignKey("users.id"))
    comment = Column(String)

class Supply(Base):
    __tablename__ = "supplies"
    id = Column(Integer, primary_key=True)
    warehouse_id = Column(Integer, ForeignKey("warehouses.id"))
    created_by = Column(Integer, ForeignKey("users.id"))
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    status = Column(String(20), default="formed")
    manager_notified_at = Column(TIMESTAMP)
    manager_confirmed_at = Column(TIMESTAMP)

class SupplyItem(Base):
    __tablename__ = "supply_items"
    id = Column(Integer, primary_key=True)
    supply_id = Column(Integer, ForeignKey("supplies.id"))
    product_id = Column(Integer, ForeignKey("products.id"))
    qty = Column(Integer, nullable=False)