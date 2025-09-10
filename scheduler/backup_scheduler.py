# scheduler/backup_scheduler.py
from __future__ import annotations

import logging

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import select

from database.db import get_session
from database.models import BackupSettings, BackupFrequency
from utils.backup import run_backup

JOB_ID = "warehouse_backup_job"
logger = logging.getLogger(__name__)


def _calc_trigger(st: BackupSettings, tzname: str) -> CronTrigger:
    tz = pytz.timezone(tzname)
    h, m = st.time_hour, st.time_minute

    if st.frequency == BackupFrequency.daily:
        return CronTrigger(hour=h, minute=m, timezone=tz)
    if st.frequency == BackupFrequency.weekly:
        # по умолчанию — понедельник; можно хранить день недели в БД
        return CronTrigger(day_of_week="mon", hour=h, minute=m, timezone=tz)
    # monthly
    return CronTrigger(day="1", hour=h, minute=m, timezone=tz)


async def reschedule_backup(scheduler: AsyncIOScheduler, tzname: str, db_url: str) -> None:
    """
    Снимает старую задачу и вешает новую по настройкам из БД (id=1).
    """
    # 1) Читаем настройки
    async with get_session() as s:
        st: BackupSettings | None = (
            (await s.execute(select(BackupSettings).where(BackupSettings.id == 1)))
            .scalar_one_or_none()
        )

    # 2) Снимаем прошлую джобу
    try:
        scheduler.remove_job(JOB_ID)
    except Exception:
        pass

    # 3) Проверяем, надо ли планировать
    if not st or not st.enabled:
        logger.info("Backups are disabled or settings missing — job not scheduled")
        return

    # 4) Считаем триггер и навешиваем джобу
    trigger = _calc_trigger(st, tzname)

    async def _job():
        ok, msg = await run_backup(db_url)
        if ok:
            logger.info(f"[BACKUP] {msg}")
        else:
            logger.error(f"[BACKUP] {msg}")
        # здесь при желании можно уведомлять админа в TG

    scheduler.add_job(_job, trigger=trigger, id=JOB_ID, replace_existing=True)
    logger.info(
        f"Backup job scheduled: {st.frequency.name} at {st.time_hour:02d}:{st.time_minute:02d} ({tzname})"
    )
