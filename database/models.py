# database/models.py
from sqlalchemy import (
    Column, Integer, String, Enum, BigInteger, TIMESTAMP,
    Boolean, ForeignKey, UniqueConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB
import enum

Base = declarative_base()


# ----- Enums -----
class UserRole(enum.Enum):
    admin = "admin"
    user = "user"
    manager = "manager"


class MovementType(enum.Enum):
    prihod = "prihod"
    korrekt = "korrekt"
    postavka = "postavka"
    # на будущее/для упаковки
    upakovka = "upakovka"


class AuditAction(enum.Enum):
    insert = "insert"
    update = "update"
    delete = "delete"


class MenuItem(enum.Enum):
    """Пункты главного меню, которыми будем управлять по ролям."""
    stocks = "stocks"
    receiving = "receiving"
    supplies = "supplies"
    packing = "packing"
    picking = "picking"   # новый раздел «Сборка»
    reports = "reports"
    admin = "admin"


class ProductStage(enum.Enum):
    """Стадия товара на складе: сырьё или упакованный."""
    raw = "raw"
    packed = "packed"


# ----- Core tables -----
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False)
    name = Column(String(255))
    role = Column(Enum(UserRole, name="user_role_enum"), nullable=False)
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
    product_id = Column(Integer, ForeignKey("products.id"))
    qty = Column(Integer, nullable=False)
    type = Column(Enum(MovementType, name="movement_type_enum"), nullable=False)
    date = Column(TIMESTAMP, server_default=func.current_timestamp())
    doc_id = Column(Integer)
    user_id = Column(Integer, ForeignKey("users.id"))
    comment = Column(String)
    # Новое поле: стадия товара (по умолчанию 'packed' для совместимости с историей)
    stage = Column(Enum(ProductStage, name="product_stage_enum"), nullable=False, default=ProductStage.packed)


class Supply(Base):
    __tablename__ = "supplies"
    id = Column(Integer, primary_key=True)
    warehouse_id = Column(Integer, ForeignKey("warehouses.id"))
    created_by = Column(Integer, ForeignKey("users.id"))
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    status = Column(String(20), default="formed")  # позже переведём на 'draft/on_picking/picked'
    manager_notified_at = Column(TIMESTAMP)
    manager_confirmed_at = Column(TIMESTAMP)


class SupplyItem(Base):
    __tablename__ = "supply_items"
    id = Column(Integer, primary_key=True)
    supply_id = Column(Integer, ForeignKey("supplies.id"))
    product_id = Column(Integer, ForeignKey("products.id"))
    qty = Column(Integer, nullable=False)


# ----- Role-based menu visibility -----
class RoleMenuVisibility(Base):
    """
    Настройки видимости пунктов главного меню по ролям.
    Админу можно хранить записи тоже (на будущее), но в логике обычно он видит всё.
    """
    __tablename__ = "role_menu_visibility"
    __table_args__ = (
        UniqueConstraint("role", "item", name="uq_role_menu_item"),
    )

    id = Column(Integer, primary_key=True)
    role = Column(Enum(UserRole, name="user_role_enum"), nullable=False)
    item = Column(Enum(MenuItem, name="menu_item_enum"), nullable=False)
    visible = Column(Boolean, nullable=False, default=True)


# ----- Audit log -----
class AuditLog(Base):
    __tablename__ = "audit_logs"
    id = Column(Integer, primary_key=True)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp(), nullable=False)

    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)  # кто сделал действие (может быть None для system)
    action = Column(Enum(AuditAction, name="audit_action_enum"), nullable=False)
    table_name = Column(String(64), nullable=False)
    record_pk = Column(String(128))  # PK в строковом виде (подходит и для составных PK)

    # снимки/разница (для insert/update/delete могут быть NULL)
    old_data = Column(JSONB)
    new_data = Column(JSONB)
    diff = Column(JSONB)
