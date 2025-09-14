# handlers/admin_backup.py
from __future__ import annotations

import sys
import asyncio
import html
import json
import os
import shlex
import shutil
import tempfile
from typing import Union

from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import (
    CallbackQuery,
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ContentType,
    FSInputFile,
)
from sqlalchemy import select
from sqlalchemy.engine.url import make_url

from config import (
    ADMIN_TELEGRAM_ID,
    TIMEZONE,
    RESTORE_SCRIPT_PATH,
    DB_URL,
)

from database.db import get_session, init_db, reset_db_engine, ping_db
from database.models import BackupSettings, BackupFrequency
from scheduler.backup_scheduler import reschedule_backup
from utils.backup import run_backup

router = Router()


# ===== FSM states =====
class BackupState(StatesGroup):
    waiting_sa_json = State()
    waiting_folder_id = State()
    waiting_time = State()
    waiting_retention = State()
    waiting_restore_file = State()
    waiting_restore_confirm = State()


# ===== helpers =====
async def _load_settings() -> BackupSettings | None:
    async with get_session() as s:
        return (
            await s.execute(select(BackupSettings).where(BackupSettings.id == 1))
        ).scalar_one_or_none()


async def _ensure_settings_exists(msg_or_cb: Union[Message, CallbackQuery]) -> BackupSettings | None:
    """
    Гарантируем, что backup_settings (id=1) существует.
    Если таблицы нет (после wipe) — создаём схему через init_db() и вставляем запись.
    """
    st = None
    try:
        st = await _load_settings()
    except Exception:
        st = None

    if not st:
        try:
            await init_db()
            async with get_session() as s:
                st = await s.get(BackupSettings, 1)
                if not st:
                    st = BackupSettings(
                        id=1,
                        enabled=False,
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
        await out.answer("⚠️ БД ещё не готова. Попробуйте позже или используйте Emergency Restore.")
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
        [InlineKeyboardButton(text=f"🧹 Retention: {st.retention_days} дн.", callback_data="bk:retention")],
        [InlineKeyboardButton(text=f"📁 Folder ID: {st.gdrive_folder_id or '—'}", callback_data="bk:folder")],
        [InlineKeyboardButton(text="🔐 Ключ сервис-аккаунта (JSON)", callback_data="bk:key")],
        [InlineKeyboardButton(text="🧪 Сделать бэкап сейчас", callback_data="bk:run")],
        [InlineKeyboardButton(text="♻️ Восстановить БД", callback_data="bk:restore")],
        [InlineKeyboardButton(text="🆘 Emergency Restore", callback_data="bk:restore_emergency")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:root")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _render(target: Union[CallbackQuery, Message], st: BackupSettings) -> None:
    text = (
        "<b>Бэкапы БД → Google Drive</b>\n\n"
        f"Статус: {'🟢 Включено' if st.enabled else '🔴 Выключено'}\n"
        f"Расписание: <code>{st.frequency.value}</code> @ {st.time_hour:02d}:{st.time_minute:02d} ({TIMEZONE})\n"
        f"Retention: {st.retention_days} дней\n"
        f"Folder ID: <code>{st.gdrive_folder_id or '—'}</code>\n"
        f"Последний запуск: {st.last_run_at.strftime('%Y-%m-%d %H:%M:%S') if st.last_run_at else '—'}\n"
        f"Статус последнего: {st.last_status or '—'}"
    )
    kb = _kb_main(st)
    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    else:
        await target.answer(text, reply_markup=kb, parse_mode="HTML")


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


# ===== schedule (частота + время) =====
def _kb_schedule_time(st: BackupSettings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Ежедневно", callback_data="bk:f:daily"),
                InlineKeyboardButton(text="Еженедельно", callback_data="bk:f:weekly"),
                InlineKeyboardButton(text="Ежемесячно", callback_data="bk:f:monthly"),
            ],
            [InlineKeyboardButton(text=f"🕒 Время: {st.time_hour:02d}:{st.time_minute:02d}", callback_data="bk:time")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:backup")],
        ]
    )


@router.callback_query(F.data == "bk:schedule")
async def bk_schedule(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    st = await _ensure_settings_exists(cb)
    if not st:
        await cb.answer()
        return
    await cb.message.edit_text("Настройка расписания:", reply_markup=_kb_schedule_time(st))
    await cb.answer()


@router.callback_query(F.data.startswith("bk:f:"))
async def bk_set_freq(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    st = await _ensure_settings_exists(cb)
    if not st:
        await cb.answer()
        return

    freq = cb.data.split(":")[-1]
    if freq not in ("daily", "weekly", "monthly"):
        await cb.answer("Неверное значение частоты.")
        return

    async with get_session() as s:
        st.frequency = {
            "daily": BackupFrequency.daily,
            "weekly": BackupFrequency.weekly,
            "monthly": BackupFrequency.monthly,
        }[freq]
        s.add(st)
        await s.commit()

    await reschedule_backup(cb.bot.scheduler, TIMEZONE, cb.bot.db_url)
    st = await _load_settings()
    await cb.message.edit_text("Настройка расписания:", reply_markup=_kb_schedule_time(st))
    await cb.answer("Частота сохранена.")


@router.callback_query(F.data == "bk:time")
async def bk_time(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    await state.set_state(BackupState.waiting_time)
    await cb.message.edit_text(
        "Введите время в формате <b>HH:MM</b> (24ч), например <code>03:15</code>.",
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(BackupState.waiting_time)
async def bk_time_set(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        return
    t = (msg.text or "").strip()
    try:
        hh_str, mm_str = t.split(":")
        hh = int(hh_str)
        mm = int(mm_str)
        assert 0 <= hh <= 23 and 0 <= mm <= 59
    except Exception:
        await msg.answer("Неверный формат. Пример: 03:15")
        return

    st = await _ensure_settings_exists(msg)
    if not st:
        return

    async with get_session() as s:
        st.time_hour = hh
        st.time_minute = mm
        s.add(st)
        await s.commit()

    await reschedule_backup(msg.bot.scheduler, TIMEZONE, msg.bot.db_url)
    await state.clear()

    st = await _load_settings()
    await msg.answer("Время сохранено.")
    await _render(msg, st)


# ===== retention =====
@router.callback_query(F.data == "bk:retention")
async def bk_retention(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    await state.set_state(BackupState.waiting_retention)
    await cb.message.edit_text(
        "Сколько дней хранить бэкапы на Google Drive? Введите число, например <code>30</code>.",
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


# ===== Folder ID =====
@router.callback_query(F.data == "bk:folder")
async def bk_folder(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    await state.set_state(BackupState.waiting_folder_id)
    await cb.message.edit_text(
        "Пришлите <b>Folder ID</b> папки Google Drive, куда складывать бэкапы.\n"
        "Пример: <code>1abcDEFghij...XYZ</code>",
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(BackupState.waiting_folder_id)
async def bk_folder_set(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        return
    folder_id = (msg.text or "").strip()
    if not folder_id:
        await msg.answer("Folder ID не должен быть пустым.")
        return

    st = await _ensure_settings_exists(msg)
    if not st:
        return

    async with get_session() as s:
        st.gdrive_folder_id = folder_id
        s.add(st)
        await s.commit()

    await state.clear()
    st = await _load_settings()
    await msg.answer("Folder ID сохранён.")
    await _render(msg, st)


# ===== Service Account JSON =====
@router.callback_query(F.data == "bk:key")
async def bk_key(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    await state.set_state(BackupState.waiting_sa_json)
    await cb.message.edit_text(
        "Пришлите <b>файл JSON</b> сервис-аккаунта Google (или вставьте JSON текстом).\n"
        "Сервис-аккаунт должен иметь доступ к папке (или поделитесь папкой с email из JSON).",
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(BackupState.waiting_sa_json, F.content_type.in_({ContentType.DOCUMENT, ContentType.TEXT}))
async def bk_key_set(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        return
    try:
        if msg.document:
            tg_file = await msg.bot.get_file(msg.document.file_id)
            content = await msg.bot.download_file(tg_file.file_path)
            raw = content.read().decode("utf-8")
        else:
            raw = (msg.text or "").strip()

        sa = json.loads(raw)
        for key in ("client_email", "private_key", "project_id"):
            if key not in sa:
                raise ValueError(f"missing {key}")
    except json.JSONDecodeError as e:
        await msg.answer(f"Не удалось прочитать JSON (decode): {e}")
        return
    except Exception as e:
        await msg.answer(f"Не удалось прочитать JSON: {e}")
        return

    st = await _ensure_settings_exists(msg)
    if not st:
        return

    async with get_session() as s:
        st.gdrive_sa_json = sa
        s.add(st)
        await s.commit()

    await state.clear()
    st = await _load_settings()
    await msg.answer("Ключ сервис-аккаунта сохранён.\n\nНе забудьте выдать доступ к папке для email из JSON.")
    await _render(msg, st)


# ===== Run backup now =====
@router.callback_query(F.data == "bk:run")
async def bk_run(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    await cb.message.edit_text("Делаю бэкап… это может занять до нескольких минут.")
    ok, msg = await run_backup(cb.bot.db_url)
    if ok:
        await cb.message.edit_text(f"✅ {msg}")
    else:
        await cb.message.edit_text(f"❌ {msg}")


# ===== Restore =====
ALLOWED_EXT = {".backup", ".backup.gz", ".dump", ".sql", ".sql.gz"}
MAX_BACKUP_SIZE_MB = 2048


def build_restore_cmd(filepath: str) -> str:
    """
    Платформенно-безопасная команда восстановления.
    На Windows-хосте всегда PowerShell, на Linux-хосте всегда linux-скрипт.
    Любые значения RESTORE_DRIVER из .env игнорируются, чтобы локалка не ломала сервер.
    """
    script = RESTORE_SCRIPT_PATH or ""
    if sys.platform.startswith("win"):
        # Windows: PowerShell-скрипт
        return f'powershell -NoProfile -ExecutionPolicy Bypass -File "{script}" -BackupPath "{filepath}"'
    else:
        # Linux: shell-скрипт с sudo
        quoted = shlex.quote(filepath)
        return f"sudo {shlex.quote(script)} {quoted}"


async def _restore_open_common(target: Union[CallbackQuery, Message], state: FSMContext):
    await state.clear()
    await state.set_state(BackupState.waiting_restore_file)
    text = (
        "♻️ Восстановление БД\n\n"
        "Пришлите файл бэкапа <b>документом</b>.\n"
        "Поддерживаемые форматы: <code>.backup</code>, <code>.backup.gz</code>, <code>.dump</code>, "
        "<code>.sql</code>, <code>.sql.gz</code>\n"
        "⚠️ ВНИМАНИЕ: действующая БД будет перезаписана."
    )
    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, parse_mode="HTML")
        await target.answer()
    else:
        await target.answer(text, parse_mode="HTML")


# Обычный сценарий (через меню)
@router.callback_query(F.data == "bk:restore")
async def bk_restore_open(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    # Блокируем восстановление не на сервере
    if os.environ.get("HOST_ROLE") and os.environ["HOST_ROLE"] != "server":
        await cb.message.edit_text("Восстановление разрешено только на сервере (HOST_ROLE != server).")
        await cb.answer()
        return
    await _ensure_settings_exists(cb)  # не критично, но подтянем настройки
    await _restore_open_common(cb, state)


# Emergency Restore (без обращения к БД)
@router.callback_query(F.data == "bk:restore_emergency")
async def bk_restore_emergency(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    if os.environ.get("HOST_ROLE") and os.environ["HOST_ROLE"] != "server":
        await cb.message.edit_text("Восстановление разрешено только на сервере (HOST_ROLE != server).")
        await cb.answer()
        return
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

    if (msg.text or "").strip() != "Я ОТДАЮ СЕБЕ ОТЧЁТ":
        await msg.answer("Нужно написать ровно: Я ОТДАЮ СЕБЕ ОТЧЁТ")
        return

    # Охранный флаг: только сервер
    if os.environ.get("HOST_ROLE") and os.environ["HOST_ROLE"] != "server":
        await msg.answer("Восстановление разрешено только на сервере (HOST_ROLE != server).")
        await state.clear()
        return

    data = await state.get_data()
    filepath = data["filepath"]

    # Доп. проверка: если Linux — убедимся, что скрипт задан
    if not sys.platform.startswith("win"):
        if not RESTORE_SCRIPT_PATH or not os.path.exists(RESTORE_SCRIPT_PATH):
            await msg.answer("RESTORE_SCRIPT_PATH не задан или не существует на сервере.")
            await state.clear()
            try:
                shutil.rmtree(data.get("tmpdir", ""), ignore_errors=True)
            finally:
                return

    await msg.answer("Запускаю восстановление… Пришлю лог выполнения.")

    # Команда (Windows/Linux формируется в build_restore_cmd)
    cmd = build_restore_cmd(filepath)

    # Пробрасываем PG-переменные из DB_URL
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
                err = html.escape(repr(e))
                await msg.answer(
                    f"⚠️ Бэкап восстановлен, но не удалось проверить соединение:\n<pre>{err}</pre>",
                    parse_mode="HTML",
                )

    except Exception as e:
        err = html.escape(repr(e))
        try:
            await msg.answer(f"<b>Ошибка запуска восстановления</b>:\n<pre>{err}</pre>", parse_mode="HTML")
        except TelegramBadRequest:
            await msg.answer(f"Ошибка запуска восстановления: {repr(e)}")
    finally:
        try:
            shutil.rmtree(data.get("tmpdir", ""), ignore_errors=True)
        except Exception:
            pass
        await state.clear()
