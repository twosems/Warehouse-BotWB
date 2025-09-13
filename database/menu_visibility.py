from __future__ import annotations
from typing import Optional, Set, Dict, List

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import RoleMenuVisibility, UserRole, MenuItem

# Человекочитаемые подписи и порядок показа в редакторе (единая точка правды)
LABELS: Dict[MenuItem, str] = {
    MenuItem.stocks:        "📦 Остатки",
    MenuItem.receiving:     "➕ Поступление",
    MenuItem.supplies:      "🚚 Поставки",
    MenuItem.packing:       "🎁 Упаковка",
    MenuItem.picking:       "🧰 Сборка",
    MenuItem.reports:       "📈 Отчёты",
    MenuItem.purchase_cn:   "🇨🇳 Закупка CN",
    MenuItem.msk_warehouse: "🏢 Склад MSK",

    MenuItem.admin:         "⚙️ Администрирование",
}

MENU_ORDER: List[MenuItem] = [
    MenuItem.stocks,
    MenuItem.receiving,
    MenuItem.supplies,
    MenuItem.packing,
    MenuItem.picking,
    MenuItem.reports,
    MenuItem.purchase_cn,
    MenuItem.msk_warehouse,
    MenuItem.admin,
]

# Базовые дефолты (явно заданные пункты)
DEFAULT_VISIBILITY: Dict[UserRole, Dict[MenuItem, bool]] = {
    # Админ: вообще всё (включая будущие пункты — см. fallback ниже)
    UserRole.admin: {
        # можно ничего не перечислять — fallback покроет True на любые новые пункты
    },
    # Пользователь
    UserRole.user: {
        MenuItem.stocks:        True,
        MenuItem.receiving:     True,
        MenuItem.supplies:      True,
        MenuItem.packing:       True,
        MenuItem.picking:       True,
        MenuItem.reports:       True,
        MenuItem.purchase_cn:   True,
        MenuItem.msk_warehouse: True,
        MenuItem.admin:         False,
    },
    # Менеджер
    UserRole.manager: {
        MenuItem.stocks:        True,
        MenuItem.receiving:     False,
        MenuItem.supplies:      True,
        MenuItem.packing:       True,
        MenuItem.picking:       True,
        MenuItem.reports:       True,
        MenuItem.purchase_cn:   False,
        MenuItem.msk_warehouse: False,
        MenuItem.admin:         False,
    },
}

def _default_visible(role: UserRole, item: MenuItem) -> bool:
    """
    Фолбэк-дефолт для незаданных явно пунктов:
      - admin -> True (видит всё)
      - user/manager -> False (безопасный дефолт)
    """
    if role == UserRole.admin:
        return True
    return DEFAULT_VISIBILITY.get(role, {}).get(item, False)

async def ensure_menu_visibility_defaults(session: AsyncSession) -> None:
    """
    Гарантируем, что в role_menu_visibility есть записи для всех (role, item):
      - для admin: True по умолчанию,
      - для остальных ролей: False, если не задано иное в DEFAULT_VISIBILITY.
    Ничего не перезаписываем — только добавляем отсутствующие пары.
    """
    # Читаем все существующие пары (role, item)
    res = await session.execute(
        select(RoleMenuVisibility.role, RoleMenuVisibility.item)
    )
    existing = {(row[0], row[1]) for row in res.all()}

    to_add: list[RoleMenuVisibility] = []
    for role in UserRole:
        for item in MenuItem:
            if (role, item) not in existing:
                default = DEFAULT_VISIBILITY.get(role, {}).get(item, _default_visible(role, item))
                to_add.append(RoleMenuVisibility(role=role, item=item, visible=default))

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

async def get_visibility_map_for_role(
        session: AsyncSession,
        role: UserRole,
) -> Dict[MenuItem, bool]:
    """
    Возвращает словарь {MenuItem: visible} для роли — удобно для экрана редактора.
    Если каких-то пунктов внезапно нет — не беда, вернём False (но ensure_* лучше вызвать).
    """
    res = await session.execute(
        select(RoleMenuVisibility.item, RoleMenuVisibility.visible).where(
            RoleMenuVisibility.role == role
        )
    )
    data = {row[0]: row[1] for row in res.all()}
    # на всякий случай заполним отсутствующие ключи фолбэком
    for mi in MenuItem:
        data.setdefault(mi, _default_visible(role, mi))
    return data

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
        # если записи нет — создадим с корректным дефолтом или переданным value
        default = _default_visible(role, item)
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
