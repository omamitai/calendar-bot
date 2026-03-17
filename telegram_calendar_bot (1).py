#!/usr/bin/env python3
"""
Telegram Bot – Hebrew Free-Text → Google Calendar
Runs on GitHub Actions
"""

import json
import logging
import os
import re
import sys
import base64
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ══════════════════════════════════════════════
#  הגדרות – נקראות מ-GitHub Secrets
# ══════════════════════════════════════════════

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
GEMINI_API_KEY     = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL       = "gemini-flash-lite-latest"
TIMEZONE           = "Asia/Jerusalem"
GOOGLE_SCOPES      = ["https://www.googleapis.com/auth/calendar"]

# קובץ users – נשמר כ-artifact בין הרצות
USERS_FILE = Path("/tmp/users.json")

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO
)
log = logging.getLogger(__name__)

WAITING_FOR_CALENDAR_ID = 0

# ══════════════════════════════════════════════
#  שמירת משתמשים
# ══════════════════════════════════════════════

def load_users() -> dict:
    if USERS_FILE.exists():
        return json.loads(USERS_FILE.read_text("utf-8"))
    return {}

def save_users(u: dict):
    USERS_FILE.write_text(json.dumps(u, ensure_ascii=False, indent=2), "utf-8")

def get_cal_id(uid: int) -> str | None:
    return load_users().get(str(uid))

def set_cal_id(uid: int, cid: str):
    u = load_users()
    u[str(uid)] = cid
    save_users(u)

# ══════════════════════════════════════════════
#  Google Calendar – Service Account
# ══════════════════════════════════════════════

def setup_service_account() -> Path:
    """
    Decode the Service Account JSON from the env-var
    GOOGLE_SA_JSON_B64  (base64-encoded).
    """
    b64 = os.environ.get("GOOGLE_SA_JSON_B64", "")
    if not b64:
        sys.exit("❌  GOOGLE_SA_JSON_B64 is missing")

    sa_path = Path("/tmp/service_account.json")
    sa_path.write_bytes(base64.b64decode(b64))
    return sa_path


def get_calendar_service(sa_path: Path):
    creds = service_account.Credentials.from_service_account_file(
        str(sa_path), scopes=GOOGLE_SCOPES
    )
    return build("calendar", "v3", credentials=creds)


def get_sa_email(sa_path: Path) -> str:
    try:
        return json.loads(sa_path.read_text())["client_email"]
    except Exception:
        return "???"


def create_event(cal_service, calendar_id: str, ev: dict) -> dict:
    tz = ZoneInfo(TIMEZONE)
    body = {"summary": ev["title"]}

    if ev.get("location"):
        body["location"] = ev["location"]
    if ev.get("description"):
        body["description"] = ev["description"]

    if ev.get("is_all_day") or not ev.get("start_time"):
        body["start"] = {"date": ev["date"]}
        end = ev.get("end_date", ev["date"])
        end_dt = datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)
        body["end"] = {"date": end_dt.strftime("%Y-%m-%d")}
    else:
        s = datetime.strptime(
            f"{ev['date']} {ev['start_time']}", "%Y-%m-%d %H:%M"
        ).replace(tzinfo=tz)

        if ev.get("end_time"):
            e = datetime.strptime(
                f"{ev['date']} {ev['end_time']}", "%Y-%m-%d %H:%M"
            ).replace(tzinfo=tz)
            if e <= s:
                e += timedelta(days=1)
        else:
            e = s + timedelta(hours=1)

        body["start"] = {"dateTime": s.isoformat(), "timeZone": TIMEZONE}
        body["end"]   = {"dateTime": e.isoformat(), "timeZone": TIMEZONE}

    return cal_service.events().insert(
        calendarId=calendar_id, body=body
    ).execute()

# ══════════════════════════════════════════════
#  Gemini – ניתוח טקסט עברי
# ══════════════════════════════════════════════

PROMPT_TEMPLATE = """\
אתה עוזר לפענוח אירועים מטקסט חופשי בעברית.

התאריך היום: {today}
אזור זמן: Asia/Jerusalem

חלץ מההודעה את פרטי האירוע והחזר **אך ורק** JSON תקין:

{{
  "title":       "כותרת קצרה",
  "date":        "YYYY-MM-DD",
  "start_time":  "HH:MM או null",
  "end_time":    "HH:MM או null",
  "location":    "מיקום או null",
  "description": "פרטים או null",
  "is_all_day":  true/false
}}

כללים:
- "מחר" = מחר. "יום שלישי הבא" = יום שלישי הקרוב.
- ראשון=Sun שני=Mon שלישי=Tue רביעי=Wed חמישי=Thu שישי=Fri שבת=Sat
- אם אין שעה → is_all_day=true, start_time=null
- JSON בלבד! בלי markdown, בלי הסברים.

ההודעה:
{message}
"""


def parse_event(gemini_model, text: str) -> dict:
    today = datetime.now(ZoneInfo(TIMEZONE)).strftime("%Y-%m-%d (%A)")
    prompt = PROMPT_TEMPLATE.format(today=today, message=text)

    resp = gemini_model.generate_content(prompt)
    raw = resp.text.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    log.info("Gemini → %s", raw[:150])
    return json.loads(raw)

# ══════════════════════════════════════════════
#  Telegram handlers
# ══════════════════════════════════════════════

DAYS_HE = {
    "Sunday": "ראשון", "Monday": "שני", "Tuesday": "שלישי",
    "Wednesday": "רביעי", "Thursday": "חמישי",
    "Friday": "שישי", "Saturday": "שבת",
}


def confirmation_msg(ev: dict, link: str) -> str:
    d = datetime.strptime(ev["date"], "%Y-%m-%d")
    day = DAYS_HE.get(d.strftime("%A"), "")
    lines = [
        "✅ <b>האירוע נוצר!</b>", "",
        f"📌  <b>{ev['title']}</b>",
        f"📅  {ev['date']}  (יום {day})",
    ]
    if ev.get("start_time"):
        t = ev["start_time"]
        if ev.get("end_time"):
            t += f"–{ev['end_time']}"
        lines.append(f"🕐  {t}")
    else:
        lines.append("🕐  כל היום")
    if ev.get("location"):
        lines.append(f"📍  {ev['location']}")
    lines += ["", f'🔗 <a href="{link}">פתח ביומן</a>']
    return "\n".join(lines)


def make_handlers(cal_service, sa_email, gemini_mdl):
    """Build all Telegram handlers (closure over services)."""

    async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        uid = update.effective_user.id
        if get_cal_id(uid):
            await update.message.reply_text(
                f"👋 היומן מוגדר!\nשלח/י אירוע בעברית ואוסיף אותו.\n"
                f"לשינוי → /setup", parse_mode="HTML",
            )
            return ConversationHandler.END

        await update.message.reply_text(
            "👋 שלום! אני מוסיף אירועים ל-Google Calendar.\n\n"
            "<b>הגדרה (פעם אחת):</b>\n\n"
            "1️⃣  שתף/י את היומן עם:\n"
            f"<code>{sa_email}</code>\n"
            "(Google Calendar → ⚙️ ליד היומן → שיתוף → הרשאה: <b>ביצוע שינויים</b>)\n\n"
            "2️⃣  שלח/י לי את כתובת ה-Gmail שלך:",
            parse_mode="HTML",
        )
        return WAITING_FOR_CALENDAR_ID

    async def setup(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        await update.message.reply_text(
            f"🔧 שלח/י Calendar ID חדש.\n"
            f"ודא/י שיתוף עם:\n<code>{sa_email}</code>",
            parse_mode="HTML",
        )
        return WAITING_FOR_CALENDAR_ID

    async def receive_cal_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        text = update.message.text.strip()
        if "@" not in text:
            await update.message.reply_text("❌ שלח/י כתובת Gmail תקינה.")
            return WAITING_FOR_CALENDAR_ID

        msg = await update.message.reply_text("⏳ בודק גישה…")
        try:
            cal_service.calendarList().insert(body={"id": text}).execute()
        except Exception:
            pass
        try:
            cal_service.events().list(
                calendarId=text, maxResults=1, singleEvents=True
            ).execute()
        except Exception as exc:
            await msg.edit_text(
                f"❌ אין גישה ליומן.\n"
                f"ודא/י שיתוף עם:\n<code>{sa_email}</code>\n\n"
                f"שגיאה: <code>{str(exc)[:120]}</code>",
                parse_mode="HTML",
            )
            return WAITING_FOR_CALENDAR_ID

        set_cal_id(update.effective_user.id, text)
        await msg.edit_text(
            f"✅ מוכן! יומן: <code>{text}</code>\n\n"
            "עכשיו שלח/י אירוע, למשל:\n"
            '\"פגישה עם דני מחר ב-10\"',
            parse_mode="HTML",
        )
        return ConversationHandler.END

    async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "<b>📅 עזרה</b>\n\n"
            "שלח/י אירוע בעברית ואוסיף ליומן.\n\n"
            "<b>דוגמאות:</b>\n"
            "• פגישה מחר ב-10\n"
            "• יום הולדת של יעל 25 אפריל\n"
            "• הרצאה יום חמישי 14:00-16:00\n\n"
            "/start · /setup · /help · /status",
            parse_mode="HTML",
        )

    async def status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        cid = get_cal_id(update.effective_user.id)
        if cid:
            await update.message.reply_text(
                f"📅 {cid}\n🤖 {GEMINI_MODEL}\n✅ פעיל",
                parse_mode="HTML",
            )
        else:
            await update.message.reply_text("⚠️ שלח /start להגדרה.")

    async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        text = update.message.text.strip()
        if not text:
            return
        uid = update.effective_user.id
        cid = get_cal_id(uid)
        if not cid:
            await update.message.reply_text("⚠️ שלח /start קודם.")
            return

        msg = await update.message.reply_text("⏳ מעבד…")
        try:
            ev = parse_event(gemini_mdl, text)
        except Exception as exc:
            log.exception("Parse error")
            await msg.edit_text(f"❌ לא הצלחתי לפענח.\nנסה/י שוב בניסוח אחר.")
            return

        if not ev.get("title") or not ev.get("date"):
            await msg.edit_text("❌ חסר כותרת או תאריך. נסה/י שוב.")
            return

        try:
            created = create_event(cal_service, cid, ev)
        except Exception as exc:
            log.exception("Calendar error")
            await msg.edit_text(f"❌ שגיאה ביומן: {exc}")
            return

        await msg.edit_text(
            confirmation_msg(ev, created.get("htmlLink", "")),
            parse_mode="HTML",
        )

    return start, setup, receive_cal_id, help_cmd, status, handle_text

# ══════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════

def main():
    if not TELEGRAM_BOT_TOKEN:
        sys.exit("❌ TELEGRAM_BOT_TOKEN missing")
    if not GEMINI_API_KEY:
        sys.exit("❌ GEMINI_API_KEY missing")

    # Google Calendar
    sa_path = setup_service_account()
    cal_service = get_calendar_service(sa_path)
    sa_email = get_sa_email(sa_path)
    log.info("SA email: %s", sa_email)

    # Gemini
    genai.configure(api_key=GEMINI_API_KEY)
    gemini_mdl = genai.GenerativeModel(
        GEMINI_MODEL,
        generation_config={"temperature": 0.1, "max_output_tokens": 2048},
    )
    log.info("Model: %s", GEMINI_MODEL)

    # Telegram
    (
        start, setup, receive_cal_id,
        help_cmd, status, handle_text
    ) = make_handlers(cal_service, sa_email, gemini_mdl)

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CommandHandler("setup", setup),
        ],
        states={
            WAITING_FOR_CALENDAR_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_cal_id)
            ],
        },
        fallbacks=[CommandHandler("help", help_cmd)],
    ))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    log.info("Bot starting…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
