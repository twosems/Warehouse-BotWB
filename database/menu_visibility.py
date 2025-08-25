# database/menu_visibility.py
from __future__ import annotations
from typing import Optional, Set

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import RoleMenuVisibility, UserRole, MenuItem

# ДЕФОЛТЫ: что видит каждая роль из коробки
DEFAULT_VISIBILITY = {
    # Админ видит всё — автоматом покроет и будущие пункты
    UserRole.admin: {item: True for item in MenuItem},

    # Пользователь: видит основные разделы, включая новые "Закупка CN" и "Склад MSK"
    UserRole.user: {
        MenuItem.stocks:         True,
        MenuItem.receiving:      True,
        MenuItem.supplies:       True,
        MenuItem.packing:        True,
        MenuItem.picking:        True,
        MenuItem.reports:        True,
        MenuItem.purchase_cn:    True,   # 🇨🇳 Закупка CN
        MenuItem.msk_warehouse:  True,   # 🏢 Склад MSK
        MenuItem.admin:          False,
    },

    # Менеджер: по умолчанию без прав на создание/приёмку CN/MSK
    UserRole.manager: {
        MenuItem.stocks:         True,
        MenuItem.receiving:      False,
        MenuItem.supplies:       True,
        MenuItem.packing:        True,
        MenuItem.picking:        True,
        MenuItem.reports:        True,
        MenuItem.purchase_cn:    False,  # можно включить позже
        MenuItem.msk_warehouse:  False,  # можно включить позже
        MenuItem.admin:          False,
    },
}


async def ensure_menu_visibility_defaults(session: AsyncSession) -> None:
    """
    Гарантируем, что в role_menu_visibility есть записи для всех (role, item)
    согласно DEFAULT_VISIBILITY. Ничего не перезаписываем – только добавляем отсутствующее.
    """
    res = await session.execute(
        select(RoleMenuVisibility.role, RoleMenuVisibility.item)
    )
    existing = {(row[0], row[1]) for row in res.all()}

    to_add: list[RoleMenuVisibility] = []
    for role, mapping in DEFAULT_VISIBILITY.items():
        for item, visible in mapping.items():
            key = (role, item)
            if key not in existing:
                to_add.append(
                    RoleMenuVisibility(role=role, item=item, visible=visible)
                )

    if to_add:
        session.add_all(to_add)
        await session.commit()


async def get_visible_menu_items_for_role(
        session: AsyncSession,
        role: UserRole,
) -> Set[MenuItem]:
    """
    Возвращает множество пунктов меню, видимых для роли.
    """
    res = await session.execute(
        select(RoleMenuVisibility.item).where(
            RoleMenuVisibility.role == role,
            RoleMenuVisibility.visible.is_(True),
            )
    )
    return {row[0] for row in res.all()}


async def toggle_menu_visibility(
        session: AsyncSession,
        role: UserRole,
        item: MenuItem,
        value: Optional[bool] = None,
) -> bool:
    """
    Переключить видимость или поставить явное значение.
    Возвращает текущее значение после операции.
    """
    res = await session.execute(
        select(RoleMenuVisibility).where(
            RoleMenuVisibility.role == role,
            RoleMenuVisibility.item == item,
            ).limit(1)
    )
    vm = res.scalar_one_or_none()

    if vm is None:
        # если записи нет — создадим с дефолтом или переданным value
        default = DEFAULT_VISIBILITY.get(role, {}).get(item, True)
        vm = RoleMenuVisibility(
            role=role, item=item, visible=default if value is None else bool(value)
        )
        session.add(vm)
        await session.commit()
        return vm.visible

    new_val = (not vm.visible) if value is None else bool(value)
    if new_val == vm.visible:
        return vm.visible

    await session.execute(
        update(RoleMenuVisibility)
        .where(RoleMenuVisibility.id == vm.id)
        .values(visible=new_val)
    )
    await session.commit()
    return new_val
