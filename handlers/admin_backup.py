# handlers/admin_backup.py
from __future__ import annotations

import asyncio
import html
import os
import re
import shutil
import tempfile
from typing import Union

from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from sqlalchemy import select, text
from sqlalchemy.engine.url import make_url

from config import ADMIN_TELEGRAM_ID, TIMEZONE, YADISK_DIR, DB_URL

from database.db import get_session, reset_db_engine, ping_db
from database.models import BackupSettings, BackupFrequency
from scheduler.backup_scheduler import reschedule_backup
from utils.backup import run_backup, build_restore_cmd

router = Router()


# ===== FSM =====
class BackupState(StatesGroup):
    waiting_schedule = State()
    waiting_time = State()
    waiting_retention = State()
    waiting_restore_file = State()
    waiting_restore_confirm = State()
    waiting_wipe_phrase = State()
    waiting_wipe_dbname = State()


# ===== helpers =====
async def _load_settings() -> BackupSettings | None:
    async with get_session() as s:
        r = await s.execute(select(BackupSettings).limit(1))
        return r.scalar_one_or_none()


async def _ensure_settings_exists(msg_or_cb: Union[Message, CallbackQuery]) -> BackupSettings | None:
    st = await _load_settings()
    if not st:
        try:
            async with get_session() as s:
                r = await s.execute(select(BackupSettings).limit(1))
                st = r.scalar_one_or_none()
                if not st:
                    st = BackupSettings(
                        enabled=True,
                        frequency=BackupFrequency.daily,
                        time_hour=3,
                        time_minute=30,
                        retention_days=30,
                    )
                    s.add(st)
                    await s.commit()
        except Exception:
            st = None
    if not st:
        out = msg_or_cb.message if isinstance(msg_or_cb, CallbackQuery) else msg_or_cb
        await out.answer("⚠️ БД ещё не готова. Попробуйте позже.")
        return None
    return st


def _kb_main(st: BackupSettings) -> InlineKeyboardMarkup:
    onoff = "🟢 Включено" if st.enabled else "🔴 Выключено"
    freq_map = {"daily": "Ежедневно", "weekly": "Еженедельно", "monthly": "Ежемесячно"}
    freq_title = freq_map.get(st.frequency.value, st.frequency.value)
    rows = [
        [InlineKeyboardButton(text=f"{onoff} — переключить", callback_data="bk:toggle")],
        [
            InlineKeyboardButton(
                text=f"⏰ Расписание: {freq_title} {st.time_hour:02d}:{st.time_minute:02d}",
                callback_data="bk:schedule",
            )
        ],
        [InlineKeyboardButton(text=f"🧹 Хранить (дней): {st.retention_days}", callback_data="bk:retention")],
        [InlineKeyboardButton(text="🧪 Сделать бэкап сейчас", callback_data="bk:run")],
        [InlineKeyboardButton(text="♻️ Восстановить БД", callback_data="bk:restore")],
        [InlineKeyboardButton(text="🧨 Очистить базу", callback_data="bk:wipe")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:root")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _render(target: Union[CallbackQuery, Message], st: BackupSettings) -> None:
    text = (
        "<b>Бэкапы БД → Яндекс.Диск</b>\n\n"
        f"Статус: {'🟢 Включено' if st.enabled else '🔴 Выключено'}\n"
        f"Расписание: <code>{st.frequency.value}</code> @ {st.time_hour:02d}:{st.time_minute:02d} ({TIMEZONE})\n"
        f"Retention: {st.retention_days} дней\n"
        f"Папка на Я.Диске: <code>{html.escape(YADISK_DIR or '—')}</code>\n"
        f"Последний запуск: {st.last_run_at.strftime('%Y-%m-%d %H:%M:%S') if st.last_run_at else '—'}\n"
        f"Статус последнего: {st.last_status or '—'}"
    )
    kb = _kb_main(st)
    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    else:
        await target.answer(text, reply_markup=kb, parse_mode="HTML")


async def _auto_back_to_menu(target: Union[CallbackQuery, Message]) -> None:
    """Через 2 секунды перерисуем экран бэкапов."""
    await asyncio.sleep(2)
    st = await _load_settings()
    if not st:
        return
    await _render(target, st)


# ===== entry points =====
@router.message(Command("backup"))
async def backup_cmd(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_TELEGRAM_ID:
        return
    await state.clear()
    st = await _ensure_settings_exists(message)
    if not st:
        return
    await _render(message, st)


@router.callback_query(F.data == "admin:backup")
async def open_backup(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    await state.clear()
    st = await _ensure_settings_exists(cb)
    if not st:
        await cb.answer()
        return
    await _render(cb, st)
    await cb.answer()


# ===== toggle on/off =====
@router.callback_query(F.data == "bk:toggle")
async def bk_toggle(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    st = await _ensure_settings_exists(cb)
    if not st:
        await cb.answer()
        return

    async with get_session() as s:
        st.enabled = not st.enabled
        s.add(st)
        await s.commit()

    await reschedule_backup(cb.bot.scheduler, TIMEZONE, cb.bot.db_url)
    st = await _load_settings()
    await _render(cb, st)
    await cb.answer("Сохранено.")


# ===== schedule =====
def _kb_schedule() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Ежедневно", callback_data="bk:sch:daily")],
        [InlineKeyboardButton(text="🗓 Еженедельно", callback_data="bk:sch:weekly")],
        [InlineKeyboardButton(text="📆 Ежемесячно", callback_data="bk:sch:monthly")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:backup")],
    ])


@router.callback_query(F.data == "bk:schedule")
async def bk_schedule(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    await state.set_state(BackupState.waiting_schedule)
    await cb.message.edit_text("Выберите периодичность:", reply_markup=_kb_schedule())
    await cb.answer()


@router.callback_query(F.data.startswith("bk:sch:"), BackupState.waiting_schedule)
async def bk_schedule_pick(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    _, _, freq = cb.data.partition("bk:sch:")
    if freq not in {"daily", "weekly", "monthly"}:
        await cb.answer("Неизвестно.")
        return
    await state.update_data(freq=freq)
    await state.set_state(BackupState.waiting_time)
    await cb.message.edit_text("Укажите время (HH:MM). Пример: 03:15")
    await cb.answer()


@router.message(BackupState.waiting_time)
async def bk_schedule_time(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        return
    m = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*", (msg.text or ""))
    if not m:
        await msg.answer("Формат времени HH:MM, пример: 03:15")
        return
    hh, mm = int(m.group(1)), int(m.group(2))
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        await msg.answer("Часы 0..23, минуты 0..59. Пример: 03:15")
        return

    data = await state.get_data()
    freq = data.get("freq") or "daily"

    st = await _ensure_settings_exists(msg)
    if not st:
        return

    async with get_session() as s:
        st.frequency = BackupFrequency(freq)
        st.time_hour = hh
        st.time_minute = mm
        s.add(st)
        await s.commit()

    await reschedule_backup(msg.bot.scheduler, TIMEZONE, msg.bot.db_url)
    await state.clear()
    st = await _load_settings()
    await msg.answer("Расписание сохранено.")
    await _render(msg, st)


# ===== retention =====
@router.callback_query(F.data == "bk:retention")
async def bk_retention(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    await state.set_state(BackupState.waiting_retention)
    await cb.message.edit_text(
        "Сколько <b>дней</b> хранить бэкапы на Я.Диске? Введите число, например <code>30</code>.",
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(BackupState.waiting_retention)
async def bk_retention_set(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        return
    try:
        days = int((msg.text or "").strip())
        assert days >= 0
    except Exception:
        await msg.answer("Введите целое число ≥ 0.")
        return

    st = await _ensure_settings_exists(msg)
    if not st:
        return

    async with get_session() as s:
        st.retention_days = days
        s.add(st)
        await s.commit()

    await state.clear()
    st = await _load_settings()
    await msg.answer("Retention сохранён.")
    await _render(msg, st)


# ===== Run backup now =====
@router.callback_query(F.data == "bk:run")
async def bk_run(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    await cb.message.edit_text("Делаю бэкап… это может занять до нескольких минут.")
    ok, msg = await run_backup(cb.bot.db_url)
    text = f"✅ {msg}" if ok else f"❌ {msg}"
    await cb.message.edit_text(text, parse_mode="HTML")
    await _auto_back_to_menu(cb)


# ===== Restore =====
ALLOWED_EXT = {".backup", ".backup.gz", ".dump", ".sql", ".sql.gz"}
MAX_BACKUP_SIZE_MB = 2048


async def _restore_open_common(target: Union[CallbackQuery, Message], state: FSMContext):
    await state.clear()
    await state.set_state(BackupState.waiting_restore_file)
    text = (
        "♻️ Восстановление БД\n\n"
        "Пришлите файл бэкапа <b>документом</b>.\n"
        "Поддерживаемые форматы: <code>.backup</code>, <code>.backup.gz</code>, "
        "<code>.dump</code>, <code>.sql</code>, <code>.sql.gz</code>\n"
        "⚠️ ВНИМАНИЕ: действующая БД будет перезаписана."
    )
    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, parse_mode="HTML")
        await target.answer()
    else:
        await target.answer(text, parse_mode="HTML")


@router.callback_query(F.data == "bk:restore")
async def bk_restore_open(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    # Только на сервере
    if os.environ.get("HOST_ROLE") and os.environ["HOST_ROLE"] != "server":
        await cb.message.edit_text("Восстановление разрешено только на сервере (HOST_ROLE != server).")
        await cb.answer()
        await _auto_back_to_menu(cb)
        return
    await _ensure_settings_exists(cb)
    await _restore_open_common(cb, state)


@router.message(BackupState.waiting_restore_file)
async def bk_restore_file(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        return
    if not msg.document:
        await msg.answer("Пришлите файл <b>документом</b>.", parse_mode="HTML")
        return

    name = (msg.document.file_name or "").lower()
    if not any(name.endswith(ext) for ext in ALLOWED_EXT):
        await msg.answer("Неподдерживаемый формат. Разрешены: .backup, .backup.gz, .dump, .sql, .sql.gz")
        return
    if msg.document.file_size and msg.document.file_size > MAX_BACKUP_SIZE_MB * 1024 * 1024:
        await msg.answer(f"Файл слишком большой (> {MAX_BACKUP_SIZE_MB} МБ).")
        return

    tmpdir = tempfile.mkdtemp(prefix="wb_restore_")
    filepath = os.path.join(tmpdir, msg.document.file_name)
    await msg.bot.download(msg.document, destination=filepath)

    await state.update_data(tmpdir=tmpdir, filepath=filepath)
    await state.set_state(BackupState.waiting_restore_confirm)
    await msg.answer(
        "Файл получен: <code>{}</code>\n\n"
        "⚠️ Чтобы продолжить, напишите фразу: <b>Я ОТДАЮ СЕБЕ ОТЧЁТ</b>\n"
        "Иначе отправьте /cancel".format(html.escape(msg.document.file_name)),
        parse_mode="HTML",
    )


@router.message(BackupState.waiting_restore_confirm)
async def bk_restore_confirm(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        return

    # Требуем точную фразу-подтверждение
    if (msg.text or "").strip() != "Я ОТДАЮ СЕБЕ ОТЧЁТ":
        await msg.answer("Нужно написать ровно: Я ОТДАЮ СЕБЕ ОТЧЁТ")
        return

    # Только сервер
    if os.environ.get("HOST_ROLE") and os.environ["HOST_ROLE"] != "server":
        await msg.answer("Восстановление разрешено только на сервере (HOST_ROLE != server).")
        await state.clear()
        await _auto_back_to_menu(msg)
        return

    data = await state.get_data()
    filepath = data["filepath"]

    # --- Preflight RESTORE_SCRIPT_PATH (строго серверный скрипт из ENV) ---
    restore_path = os.environ.get("RESTORE_SCRIPT_PATH")
    if not restore_path or not (os.path.isfile(restore_path) and os.access(restore_path, os.X_OK)):
        await msg.answer(
            "♻️ Восстановление недоступно: RESTORE_SCRIPT_PATH не задан "
            "или файл не существует/не исполняем на сервере."
        )
        try:
            shutil.rmtree(data.get("tmpdir", ""), ignore_errors=True)
        finally:
            await state.clear()
        await _auto_back_to_menu(msg)
        return
    # --- /Preflight ---

    await msg.answer("Запускаю восстановление… Пришлю лог выполнения.")
    cmd = build_restore_cmd(filepath)

    # Пробрасываем PG-переменные из DB_URL (удобно для инструментов)
    env = os.environ.copy()
    try:
        u = make_url(DB_URL)
        if u.password:
            env["PGPASSWORD"] = u.password
        if u.username:
            env["PGUSER"] = u.username
        if u.host:
            env["PGHOST"] = u.host
        if u.port:
            env["PGPORT"] = str(u.port)
        if u.database:
            env["PGDATABASE"] = u.database
    except Exception:
        pass

    try:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env,
        )
        out, _ = await proc.communicate()
        code = proc.returncode
        log = out.decode(errors="ignore") if out else "(нет вывода)"

        # Если лог длинный — отправляем файлом
        if len(log) > 3500:
            logpath = os.path.join(data["tmpdir"], "restore.log")
            with open(logpath, "w", encoding="utf-8") as f:
                f.write(log)
            await msg.answer_document(FSInputFile(logpath), caption=f"Код завершения: {code}")
        else:
            safe = html.escape(log)
            text = f"Код завершения: {code}\n\n<pre>{safe}</pre>"
            try:
                await msg.answer(text, parse_mode="HTML")
            except TelegramBadRequest:
                logpath = os.path.join(data["tmpdir"], "restore.log")
                with open(logpath, "w", encoding="utf-8") as f:
                    f.write(log)
                await msg.answer_document(FSInputFile(logpath), caption=f"Код завершения: {code}")

        # Пересобираем пул и пингуем БД ТОЛЬКО при успешном восстановлении
        if code == 0:
            try:
                await reset_db_engine()
                await ping_db()
                await msg.answer("✅ Пул подключений к БД пересоздан, соединение проверено.")
            except Exception as e:
                await msg.answer(
                    f"⚠️ Бэкап восстановлен, но не удалось проверить соединение:\n<pre>{html.escape(repr(e))}</pre>",
                    parse_mode="HTML",
                )

    except Exception as e:
        try:
            await msg.answer(
                f"<b>Ошибка запуска восстановления</b>:\n<pre>{html.escape(repr(e))}</pre>",
                parse_mode="HTML",
            )
        except TelegramBadRequest:
            await msg.answer(f"Ошибка запуска восстановления: {repr(e)}")
    finally:
        try:
            shutil.rmtree(data.get("tmpdir", ""), ignore_errors=True)
        except Exception:
            pass
        await state.clear()
        await _auto_back_to_menu(msg)


# ===== Очистка базы (wipe) =====
def _mask_db_url() -> tuple[str, str]:
    """Вернёт (маскированный URI, имя БД)."""
    try:
        u = make_url(DB_URL)
        safe = u.render_as_string(hide_password=True)
        dbname = u.database or ""
        return safe, dbname
    except Exception:
        return "(не удалось разобрать DB_URL)", ""


@router.callback_query(F.data == "bk:wipe")
async def bk_wipe(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return

    # Только на сервере
    if os.environ.get("HOST_ROLE") and os.environ["HOST_ROLE"] != "server":
        await cb.message.edit_text("Очистка базы разрешена только на сервере (HOST_ROLE != server).")
        await cb.answer()
        await _auto_back_to_menu(cb)
        return

    safe_url, dbname = _mask_db_url()
    await state.set_state(BackupState.waiting_wipe_phrase)
    await state.update_data(dbname=dbname)
    await cb.message.edit_text(
        "🧨 <b>ОЧИСТКА БАЗЫ ДАННЫХ</b>\n\n"
        f"Текущая БД: <code>{html.escape(safe_url)}</code>\n\n"
        "⚠️ Будет удалено ВСЁ содержимое схемы <code>public</code>.\n\n"
        "Чтобы продолжить, напишите ровно: <b>Я ПОДТВЕРЖДАЮ ОЧИСТКУ БД</b>\n"
        "Или нажмите /cancel",
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(BackupState.waiting_wipe_phrase)
async def bk_wipe_phrase(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        return
    if (msg.text or "").strip() != "Я ПОДТВЕРЖДАЮ ОЧИСТКУ БД":
        await msg.answer("Нужно написать ровно: Я ПОДТВЕРЖДАЮ ОЧИСТКУ БД")
        return

    data = await state.get_data()
    dbname = data.get("dbname") or ""
    if not dbname:
        _, dbname = _mask_db_url()

    await state.set_state(BackupState.waiting_wipe_dbname)
    await msg.answer(
        "Последний шаг.\n"
        f"Введите имя БД для подтверждения: <code>{html.escape(dbname or '(не удалось определить)')}</code>",
        parse_mode="HTML",
    )


@router.message(BackupState.waiting_wipe_dbname)
async def bk_wipe_do(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        return

    data = await state.get_data()
    expected_db = data.get("dbname") or _mask_db_url()[1]
    provided = (msg.text or "").strip()

    if not expected_db or provided != expected_db:
        await msg.answer("Имя БД не совпало. Отменено.")
        await state.clear()
        await _auto_back_to_menu(msg)
        return

    # Чистим БЕЗ пересоздания схемы: TRUNCATE всех таблиц public с каскадом и сбросом идентификаторов
    sql_truncate_all = text("""
DO $$
DECLARE
    stmt text;
BEGIN
    SELECT 'TRUNCATE TABLE ' ||
           string_agg(format('%I.%I', n.nspname, c.relname), ', ')
           || ' RESTART IDENTITY CASCADE'
      INTO stmt
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
      AND n.nspname = 'public';

    IF stmt IS NOT NULL THEN
        EXECUTE stmt;
    END IF;
END $$;
""")

    try:
        async with get_session() as s:
            await s.execute(sql_truncate_all)
            await s.commit()

        # Пул на всякий случай пересоздадим и проверим подключение
        await reset_db_engine()
        await ping_db()

        await msg.answer("✅ База очищена (TRUNCATE … RESTART IDENTITY CASCADE).")
    except Exception as e:
        safe = html.escape(repr(e))
        await msg.answer(f"❌ Ошибка очистки: <pre>{safe}</pre>", parse_mode="HTML")
    finally:
        await state.clear()
        await _auto_back_to_menu(msg)
