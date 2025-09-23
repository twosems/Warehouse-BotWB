from __future__ import annotations

import enum
from datetime import datetime
from typing import Optional, List

from sqlalchemy import (
    Column, Integer, String, Enum, BigInteger, TIMESTAMP, Boolean,
    ForeignKey, UniqueConstraint, Numeric, DateTime,
)
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy.sql import func

Base = declarative_base()

# ===== Enums =====

class UserRole(enum.Enum):
    admin = "admin"
    user = "user"
    manager = "manager"


class MovementType(enum.Enum):
    prihod = "prihod"
    korrekt = "korrekt"
    postavka = "postavka"
    upakovka = "upakovka"  # для упаковки


class AuditAction(enum.Enum):
    insert = "insert"
    update = "update"
    delete = "delete"


class MenuItem(enum.Enum):
    stocks = "stocks"
    receiving = "receiving"
    supplies = "supplies"
    packing = "packing"
    picking = "picking"
    reports = "reports"
    purchase_cn = "purchase_cn"
    msk_warehouse = "msk_warehouse"
    admin = "admin"


class ProductStage(enum.Enum):
    raw = "raw"
    packed = "packed"


# ——— Новые enum'ы для закупки CN и входящих МСК ———

class CnPurchaseStatus(enum.Enum):
    PURCHASED = "1_purchased"
    SENT_TO_CARGO = "2_sent_to_cargo"
    SENT_TO_MSK = "3_sent_to_msk"
    DELIVERED_TO_MSK = "4_delivered_to_msk"


class MskInboundStatus(enum.Enum):
    PENDING = "pending"
    RECEIVED = "received"


# ===== Core tables =====

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
    stage = Column(
        Enum(ProductStage, name="product_stage_enum"),
        nullable=False,
        default=ProductStage.packed
    )


# ===== SUPPLIES (ТЗ v1.0) =====

class SupplyStatus(enum.Enum):
    draft = "draft"
    queued = "queued"
    assembling = "assembling"
    assembled = "assembled"
    in_transit = "in_transit"
    archived_delivered = "archived_delivered"
    archived_returned = "archived_returned"
    cancelled = "cancelled"


class Supply(Base):
    __tablename__ = "supplies"
    id = Column(Integer, primary_key=True)
    warehouse_id = Column(Integer, ForeignKey("warehouses.id"))
    created_by = Column(Integer, ForeignKey("users.id"))
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())

    status = Column(
        SAEnum(
            SupplyStatus,
            name="supply_status",
            create_type=True,   # включаем автосоздание типа
            native_enum=True
        ),
        nullable=False,
        default=SupplyStatus.draft
    )

    mp = Column(String(16))                # 'wb' | 'ozon'
    mp_warehouse = Column(String(128))     # код/имя МП-склада
    assigned_picker_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    comment = Column(String)

    queued_at = Column(DateTime)
    assembled_at = Column(DateTime)
    posted_at = Column(DateTime)
    delivered_at = Column(DateTime)
    returned_at = Column(DateTime)
    unposted_at = Column(DateTime)

    items: Mapped[List["SupplyItem"]] = relationship(
        back_populates="supply", cascade="all, delete-orphan"
    )


class SupplyBox(Base):
    __tablename__ = "supply_boxes"
    id = Column(Integer, primary_key=True)
    supply_id = Column(Integer, ForeignKey("supplies.id", ondelete="CASCADE"), index=True, nullable=False)
    box_number = Column(Integer, nullable=False)
    sealed = Column(Boolean, nullable=False, default=False)
    # items связь не обязательна в ORM, используем из SupplyItem.box_id


class SupplyItem(Base):
    __tablename__ = "supply_items"
    id = Column(Integer, primary_key=True)
    supply_id = Column(Integer, ForeignKey("supplies.id"))
    product_id = Column(Integer, ForeignKey("products.id"))
    qty = Column(Integer, nullable=False)
    box_id = Column(Integer, ForeignKey("supply_boxes.id", ondelete="CASCADE"), nullable=True)

    supply: Mapped["Supply"] = relationship(back_populates="items")


class SupplyFile(Base):
    __tablename__ = "supply_files"
    id = Column(Integer, primary_key=True)
    supply_id = Column(Integer, ForeignKey("supplies.id", ondelete="CASCADE"), index=True, nullable=False)
    file_id = Column(String(256), nullable=False)  # Telegram file_id
    filename = Column(String(255))
    uploaded_by = Column(Integer, ForeignKey("users.id"))
    uploaded_at = Column(DateTime, server_default=func.current_timestamp())


# ===== Role-based menu visibility =====

class RoleMenuVisibility(Base):
    __tablename__ = "role_menu_visibility"
    __table_args__ = (UniqueConstraint("role", "item", name="uq_role_menu_item"),)

    id = Column(Integer, primary_key=True)
    role = Column(Enum(UserRole, name="user_role_enum"), nullable=False)
    item = Column(Enum(MenuItem, name="menu_item_enum"), nullable=False)
    visible = Column(Boolean, nullable=False, default=True)


# ===== Audit log =====

class AuditLog(Base):
    __tablename__ = "audit_logs"
    id = Column(Integer, primary_key=True)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp(), nullable=False)

    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    action = Column(Enum(AuditAction, name="audit_action_enum"), nullable=False)
    table_name = Column(String(64), nullable=False)
    record_pk = Column(String(128))
    old_data = Column(JSONB)
    new_data = Column(JSONB)
    diff = Column(JSONB)


# ===== Упаковка (Pack Docs) =====

class PackDocStatus(enum.Enum):
    draft = "draft"
    posted = "posted"


class PackDoc(Base):
    __tablename__ = "pack_docs"

    id: Mapped[int] = mapped_column(primary_key=True)
    number: Mapped[str] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"))
    user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"))

    # Новая колонка
    notes: Mapped[Optional[str]] = mapped_column(String(255))

    status: Mapped[PackDocStatus] = mapped_column(
        SAEnum(
            PackDocStatus,
            name="pack_doc_status_enum",   # стабильное имя типа
            create_type=True,              # ORM создаст тип, если его нет
            native_enum=True
        ),
        default=PackDocStatus.draft,
        nullable=False
    )

    comment: Mapped[Optional[str]] = mapped_column(String(255))

    warehouse: Mapped["Warehouse"] = relationship()
    items: Mapped[List["PackDocItem"]] = relationship(
        back_populates="doc",
        cascade="all, delete-orphan"
    )

class PackDocItem(Base):
    __tablename__ = "pack_doc_items"

    id: Mapped[int] = mapped_column(primary_key=True)
    doc_id: Mapped[int] = mapped_column(ForeignKey("pack_docs.id"))
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))
    qty: Mapped[int]

    doc: Mapped["PackDoc"] = relationship(back_populates="items")
    product: Mapped["Product"] = relationship()


# ===== Закупка CN =====

class CnPurchase(Base):
    __tablename__ = "cn_purchases"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(40), unique=True, nullable=False)
    status: Mapped[CnPurchaseStatus] = mapped_column(
        Enum(
            CnPurchaseStatus,
            name="cn_purchase_status",
            values_callable=lambda enum_cls: [e.value for e in enum_cls],
        ),
        default=CnPurchaseStatus.PURCHASED,
        nullable=False,
    )

    comment: Mapped[Optional[str]]
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    created_by_user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"))
    updated_at: Mapped[Optional[datetime]] = mapped_column(default=None, onupdate=datetime.utcnow)
    updated_by_user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"))

    sent_to_cargo_at = Column(DateTime, nullable=True)
    sent_to_msk_at   = Column(DateTime, nullable=True)
    archived_at      = Column(DateTime, nullable=True)

    items: Mapped[List["CnPurchaseItem"]] = relationship(
        back_populates="purchase", cascade="all, delete-orphan"
    )

    photos: Mapped[List["CnPurchasePhoto"]] = relationship(
        back_populates="purchase", cascade="all, delete-orphan", lazy="selectin"
    )

    msk_inbound: Mapped[Optional["MskInboundDoc"]] = relationship(
        back_populates="cn_purchase", uselist=False, cascade="all, delete-orphan"
    )


class CnPurchaseItem(Base):
    __tablename__ = "cn_purchase_items"

    id: Mapped[int] = mapped_column(primary_key=True)
    cn_purchase_id: Mapped[int] = mapped_column(ForeignKey("cn_purchases.id", ondelete="CASCADE"))
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))
    qty: Mapped[int]
    unit_cost_rub: Mapped[Numeric] = mapped_column(Numeric(12, 2))
    comment: Mapped[Optional[str]]
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)

    purchase: Mapped["CnPurchase"] = relationship(back_populates="items")
    product: Mapped["Product"] = relationship()


class CnPurchasePhoto(Base):
    __tablename__ = "cn_purchase_photos"

    id: Mapped[int] = mapped_column(primary_key=True)
    cn_purchase_id: Mapped[int] = mapped_column(
        ForeignKey("cn_purchases.id", ondelete="CASCADE"),
        index=True, nullable=False
    )
    file_id: Mapped[str] = mapped_column(String(256), nullable=False)
    caption: Mapped[Optional[str]] = mapped_column(String(512))
    uploaded_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    uploaded_by_user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"))

    purchase: Mapped["CnPurchase"] = relationship(back_populates="photos")


# ===== Входящие МСК =====

class MskInboundDoc(Base):
    __tablename__ = "msk_inbound_docs"

    id: Mapped[int] = mapped_column(primary_key=True)
    cn_purchase_id: Mapped[int] = mapped_column(
        ForeignKey("cn_purchases.id", ondelete="CASCADE"),
        unique=True, nullable=False
    )
    status: Mapped[MskInboundStatus] = mapped_column(
        Enum(
            MskInboundStatus,
            name="msk_inbound_status",
            values_callable=lambda enum_cls: [e.value for e in enum_cls],
        ),
        default=MskInboundStatus.PENDING,
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    created_by_user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"))

    warehouse_id: Mapped[Optional[int]] = mapped_column(
        "target_warehouse_id", ForeignKey("warehouses.id"), nullable=True
    )

    to_our_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    received_at: Mapped[Optional[datetime]]
    received_by_user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"))
    comment: Mapped[Optional[str]]

    cn_purchase: Mapped["CnPurchase"] = relationship(back_populates="msk_inbound")
    items: Mapped[List["MskInboundItem"]] = relationship(
        back_populates="doc", cascade="all, delete-orphan"
    )

    warehouse: Mapped[Optional["Warehouse"]] = relationship(
        "Warehouse", foreign_keys=[warehouse_id], lazy="joined"
    )


class MskInboundItem(Base):
    __tablename__ = "msk_inbound_items"

    id: Mapped[int] = mapped_column(primary_key=True)
    msk_inbound_id: Mapped[int] = mapped_column(ForeignKey("msk_inbound_docs.id", ondelete="CASCADE"))
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))
    qty: Mapped[int]
    unit_cost_rub: Mapped[Numeric] = mapped_column(Numeric(12, 2))
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)

    doc: Mapped["MskInboundDoc"] = relationship(back_populates="items")
    product: Mapped["Product"] = relationship()


# ===== Настройки бэкапов =====

class BackupFrequency(enum.Enum):
    daily = "daily"
    weekly = "weekly"
    monthly = "monthly"


class BackupSettings(Base):
    __tablename__ = "backup_settings"
    id = Column(Integer, primary_key=True, default=1)
    enabled = Column(Boolean, nullable=False, default=False)
    frequency = Column(Enum(BackupFrequency, name="backup_frequency_enum"), nullable=False, default=BackupFrequency.daily)
    time_hour = Column(Integer, nullable=False, default=3)
    time_minute = Column(Integer, nullable=False, default=15)
    retention_days = Column(Integer, nullable=False, default=30)
    gdrive_folder_id = Column(String(128))
    gdrive_sa_json = Column(JSONB)
    last_run_at = Column(TIMESTAMP)
    last_status = Column(String(255))
