#!/usr/bin/env python3
"""
Telegram Calendar Bot — Hebrew → Google Calendar
GitHub Actions edition
"""

import json, logging, os, re, sys, base64, io
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, ConversationHandler,
    MessageHandler, CallbackQueryHandler, ContextTypes, filters,
)

# ═══════════════════════════════════════
#  Config (from GitHub Secrets / env)
# ═══════════════════════════════════════

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
GEMINI_API_KEY     = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL       = "gemini-flash-lite-latest"
TIMEZONE           = "Asia/Jerusalem"
SCOPES             = ["https://www.googleapis.com/auth/calendar"]
USERS_FILE         = Path("/tmp/users.json")
HISTORY_FILE       = Path("/tmp/event_history.json")

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

STATE_CAL_ID = 0

# ═══════════════════════════════════════
#  Persistence helpers
# ═══════════════════════════════════════

def _load(path: Path) -> dict:
    return json.loads(path.read_text("utf-8")) if path.exists() else {}

def _save(path: Path, data: dict):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")

def get_cal_id(uid: int) -> str | None:
    return _load(USERS_FILE).get(str(uid))

def set_cal_id(uid: int, cid: str):
    d = _load(USERS_FILE); d[str(uid)] = cid; _save(USERS_FILE, d)

def save_event_to_history(uid: int, event_id: str, calendar_id: str, event_data: dict):
    h = _load(HISTORY_FILE)
    key = str(uid)
    if key not in h:
        h[key] = []
    h[key].append({
        "event_id": event_id,
        "calendar_id": calendar_id,
        "data": event_data,
        "created_at": datetime.now(ZoneInfo(TIMEZONE)).isoformat(),
    })
    # keep last 50 per user
    h[key] = h[key][-50:]
    _save(HISTORY_FILE, h)

def get_last_event(uid: int) -> dict | None:
    h = _load(HISTORY_FILE)
    events = h.get(str(uid), [])
    return events[-1] if events else None

def pop_last_event(uid: int) -> dict | None:
    h = _load(HISTORY_FILE)
    key = str(uid)
    events = h.get(key, [])
    if not events:
        return None
    ev = events.pop()
    _save(HISTORY_FILE, h)
    return ev

# ═══════════════════════════════════════
#  Google Calendar
# ═══════════════════════════════════════

def init_google():
    b64 = os.environ.get("GOOGLE_SA_JSON_B64", "")
    if not b64:
        sys.exit("GOOGLE_SA_JSON_B64 missing")
    sa_path = Path("/tmp/sa.json")
    sa_path.write_bytes(base64.b64decode(b64))
    creds = service_account.Credentials.from_service_account_file(str(sa_path), scopes=SCOPES)
    service = build("calendar", "v3", credentials=creds)
    email = json.loads(sa_path.read_text()).get("client_email", "?")
    return service, email


def create_event(svc, cal_id: str, ev: dict) -> dict:
    tz = ZoneInfo(TIMEZONE)
    body = {"summary": ev["title"]}
    if ev.get("location"):   body["location"]    = ev["location"]
    if ev.get("description"): body["description"] = ev["description"]

    if ev.get("is_all_day") or not ev.get("start_time"):
        body["start"] = {"date": ev["date"]}
        end = datetime.strptime(ev.get("end_date", ev["date"]), "%Y-%m-%d") + timedelta(days=1)
        body["end"] = {"date": end.strftime("%Y-%m-%d")}
    else:
        s = datetime.strptime(f"{ev['date']} {ev['start_time']}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
        if ev.get("end_time"):
            e = datetime.strptime(f"{ev['date']} {ev['end_time']}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
            if e <= s: e += timedelta(days=1)
        else:
            e = s + timedelta(hours=1)
        body["start"] = {"dateTime": s.isoformat(), "timeZone": TIMEZONE}
        body["end"]   = {"dateTime": e.isoformat(), "timeZone": TIMEZONE}

    return svc.events().insert(calendarId=cal_id, body=body).execute()


def delete_event(svc, cal_id: str, event_id: str):
    svc.events().delete(calendarId=cal_id, eventId=event_id).execute()

# ═══════════════════════════════════════
#  Gemini LLM
# ═══════════════════════════════════════

PARSE_PROMPT = """\
You are an event extraction engine. Extract calendar event details from the user input.

Today: {today}
Timezone: Asia/Jerusalem

Return ONLY a valid JSON object, no markdown, no explanation:
{{
  "title": "short title",
  "date": "YYYY-MM-DD",
  "start_time": "HH:MM or null",
  "end_time": "HH:MM or null",
  "location": "location or null",
  "description": "extra details or null",
  "is_all_day": true/false
}}

Day mapping: ראשון=Sun שני=Mon שלישי=Tue רביעי=Wed חמישי=Thu שישי=Fri שבת=Sat
"מחר" = tomorrow. "יום X הבא" = next X.
No time mentioned → is_all_day=true, start_time=null.
JSON only!

Input:
{input}
"""

IMAGE_PROMPT = """\
You are an event extraction engine. The user sent an image (screenshot, photo of a poster/flyer, WhatsApp message, etc.).

Examine the image carefully. Extract ANY calendar event information you can find: dates, times, titles, locations.

Today: {today}
Timezone: Asia/Jerusalem

Return ONLY a valid JSON object, no markdown, no explanation:
{{
  "title": "short title",
  "date": "YYYY-MM-DD",
  "start_time": "HH:MM or null",
  "end_time": "HH:MM or null",
  "location": "location or null",
  "description": "extra details or null",
  "is_all_day": true/false
}}

If the image contains Hebrew, interpret it. If you see multiple events, extract the FIRST/MAIN one.
JSON only!
"""


def init_gemini():
    genai.configure(api_key=GEMINI_API_KEY)
    return genai.GenerativeModel(
        GEMINI_MODEL,
        generation_config={"temperature": 0.1, "max_output_tokens": 2048},
    )


def _clean_json(raw: str) -> dict:
    raw = raw.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    return json.loads(raw)


def parse_text(model, text: str) -> dict:
    today = datetime.now(ZoneInfo(TIMEZONE)).strftime("%Y-%m-%d (%A)")
    resp = model.generate_content(PARSE_PROMPT.format(today=today, input=text))
    return _clean_json(resp.text)


def parse_image(model, image_bytes: bytes, caption: str = "") -> dict:
    today = datetime.now(ZoneInfo(TIMEZONE)).strftime("%Y-%m-%d (%A)")
    prompt = IMAGE_PROMPT.format(today=today)
    if caption:
        prompt += f"\n\nUser also wrote: {caption}"

    import PIL.Image
    img = PIL.Image.open(io.BytesIO(image_bytes))

    resp = model.generate_content([prompt, img])
    return _clean_json(resp.text)

# ═══════════════════════════════════════
#  Formatting
# ═══════════════════════════════════════

_DAYS = {
    "Sunday": "ראשון", "Monday": "שני", "Tuesday": "שלישי",
    "Wednesday": "רביעי", "Thursday": "חמישי",
    "Friday": "שישי", "Saturday": "שבת",
}

def _fmt_date(date_str: str) -> str:
    d = datetime.strptime(date_str, "%Y-%m-%d")
    day = _DAYS.get(d.strftime("%A"), "")
    return f"{d.strftime('%d.%m.%Y')} · יום {day}"

def _fmt_time(ev: dict) -> str:
    if ev.get("is_all_day") or not ev.get("start_time"):
        return "כל היום"
    t = ev["start_time"]
    if ev.get("end_time"):
        t += f" — {ev['end_time']}"
    return t

def fmt_event_card(ev: dict, link: str = "") -> str:
    """Professional event confirmation card."""
    lines = [
        "╭───────────────────────╮",
        f"  ✓  <b>{ev['title']}</b>",
        "╰───────────────────────╯",
        "",
        f"  📅  {_fmt_date(ev['date'])}",
        f"  🕐  {_fmt_time(ev)}",
    ]
    if ev.get("location"):
        lines.append(f"  📍  {ev['location']}")
    if ev.get("description"):
        lines.append(f"  📝  {ev['description']}")
    if link:
        lines += ["", f"  <a href=\"{link}\">פתח ב-Google Calendar →</a>"]
    return "\n".join(lines)


def fmt_preview_card(ev: dict) -> str:
    """Preview before confirming (used in undo context)."""
    lines = [
        f"<b>{ev['title']}</b>",
        f"📅 {_fmt_date(ev['date'])}  ·  🕐 {_fmt_time(ev)}",
    ]
    if ev.get("location"):
        lines.append(f"📍 {ev['location']}")
    return "\n".join(lines)

# ═══════════════════════════════════════
#  Telegram Handlers
# ═══════════════════════════════════════

def build_app(cal_svc, sa_email, gemini_mdl):

    # ── Onboarding ──

    async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        uid = update.effective_user.id
        if get_cal_id(uid):
            name = update.effective_user.first_name or ""
            await update.message.reply_text(
                f"👋  <b>{name}, הבוט מוכן.</b>\n\n"
                "שלח הודעה או צילום מסך — אוסיף ליומן.\n\n"
                "<code>/setup</code> לשינוי יומן  ·  <code>/help</code> לעזרה",
                parse_mode="HTML",
            )
            return ConversationHandler.END

        await update.message.reply_text(
            "👋  <b>שלום! בוא נחבר את היומן.</b>\n\n"
            "① שתף את היומן עם הכתובת:\n"
            f"<code>{sa_email}</code>\n\n"
            "<i>Google Calendar → ⚙️ ליד היומן → שיתוף</i>\n"
            "<i>→ הרשאה: ביצוע שינויים באירועים</i>\n\n"
            "② שלח לי את כתובת ה-Gmail:",
            parse_mode="HTML",
        )
        return STATE_CAL_ID

    async def cmd_setup(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        await update.message.reply_text(
            "🔧  <b>הגדרת יומן</b>\n\n"
            f"ודא שיתוף עם: <code>{sa_email}</code>\n"
            "שלח Calendar ID:",
            parse_mode="HTML",
        )
        return STATE_CAL_ID

    async def recv_cal_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        text = update.message.text.strip()
        if "@" not in text:
            await update.message.reply_text(
                "✖  לא נראה כמו כתובת תקינה. נסה שוב:"
            )
            return STATE_CAL_ID

        msg = await update.message.reply_text("⠋ בודק גישה…")

        try:
            cal_svc.calendarList().insert(body={"id": text}).execute()
        except Exception:
            pass

        try:
            cal_svc.events().list(calendarId=text, maxResults=1, singleEvents=True).execute()
        except Exception as exc:
            await msg.edit_text(
                "✖  <b>אין גישה ליומן.</b>\n\n"
                f"ודא שיתוף עם:\n<code>{sa_email}</code>\n\n"
                f"<i>{str(exc)[:100]}</i>",
                parse_mode="HTML",
            )
            return STATE_CAL_ID

        set_cal_id(update.effective_user.id, text)
        await msg.edit_text(
            f"✓  <b>יומן מחובר:</b> <code>{text}</code>\n\n"
            "שלח אירוע בעברית או צילום מסך — ואני מוסיף ליומן.",
            parse_mode="HTML",
        )
        return ConversationHandler.END

    # ── Help / Status ──

    async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "<b>📅  בוט יומן — עזרה</b>\n\n"

            "<b>טקסט</b> — כתוב אירוע בשפה חופשית:\n"
            "<i>  \"פגישה עם דני מחר ב-10 בבוקר\"</i>\n"
            "<i>  \"יום הולדת יעל 25 אפריל\"</i>\n"
            "<i>  \"הרצאה יום חמישי 14:00–16:00\"</i>\n\n"

            "<b>תמונה</b> — שלח צילום מסך, פלייר, או הודעת WhatsApp.\n"
            "אזהה אוטומטית את פרטי האירוע.\n\n"

            "<b>ביטול</b> — אחרי כל אירוע אפשר ללחוץ \"בטל\" או לשלוח /undo.\n\n"

            "<b>פקודות</b>\n"
            "/start · /setup · /help · /status · /undo",
            parse_mode="HTML",
        )

    async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        cid = get_cal_id(update.effective_user.id)
        if cid:
            await update.message.reply_text(
                f"<b>סטטוס</b>\n\n"
                f"📅  <code>{cid}</code>\n"
                f"🤖  <code>{GEMINI_MODEL}</code>\n"
                f"✓  פעיל",
                parse_mode="HTML",
            )
        else:
            await update.message.reply_text("לא מחובר. שלח /start.")

    # ── Undo ──

    async def cmd_undo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await _do_undo(update.effective_user.id, update.message.reply_text)

    async def callback_undo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        uid = query.from_user.id
        ev = pop_last_event(uid)
        if not ev:
            await query.edit_message_reply_markup(reply_markup=None)
            await query.message.reply_text("אין מה לבטל.")
            return

        try:
            delete_event(cal_svc, ev["calendar_id"], ev["event_id"])
            # Edit the original message to show it was cancelled
            title = ev["data"].get("title", "?")
            await query.edit_message_text(
                f"<s>{title}</s>  —  <b>בוטל ✓</b>",
                parse_mode="HTML",
            )
        except Exception as exc:
            log.exception("Delete failed")
            await query.edit_message_reply_markup(reply_markup=None)
            await query.message.reply_text(f"שגיאה במחיקה: {exc}")

    async def _do_undo(uid: int, reply_fn):
        ev = pop_last_event(uid)
        if not ev:
            await reply_fn("אין אירוע אחרון לביטול.")
            return
        try:
            delete_event(cal_svc, ev["calendar_id"], ev["event_id"])
            title = ev["data"].get("title", "?")
            await reply_fn(
                f"<s>{title}</s>  —  <b>בוטל ✓</b>",
                parse_mode="HTML",
            )
        except Exception as exc:
            log.exception("Delete failed")
            await reply_fn(f"שגיאה: {exc}")

    # ── Core: process parsed event ──

    async def _process_event(uid: int, ev: dict, edit_msg) -> None:
        """Validate, create event, send confirmation."""
        cal_id = get_cal_id(uid)
        if not cal_id:
            await edit_msg("שלח /start קודם.")
            return

        if not ev.get("title") or not ev.get("date"):
            await edit_msg("✖  לא זיהיתי כותרת או תאריך. נסה שוב.")
            return

        # Validate date format
        try:
            datetime.strptime(ev["date"], "%Y-%m-%d")
        except ValueError:
            await edit_msg("✖  תאריך לא תקין. נסה שוב.")
            return

        try:
            created = create_event(cal_svc, cal_id, ev)
        except Exception as exc:
            log.exception("Calendar create error")
            await edit_msg(f"✖  שגיאה ביומן: {exc}")
            return

        event_id = created.get("id", "")
        link = created.get("htmlLink", "")

        save_event_to_history(uid, event_id, cal_id, ev)

        card = fmt_event_card(ev, link)
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("↩ בטל אירוע", callback_data="undo")]
        ])

        await edit_msg(card, parse_mode="HTML", reply_markup=keyboard)

    # ── Text handler ──

    async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        text = update.message.text.strip()
        if not text:
            return
        uid = update.effective_user.id
        if not get_cal_id(uid):
            await update.message.reply_text("שלח /start כדי להתחיל.")
            return

        msg = await update.message.reply_text("⠋")

        try:
            ev = parse_text(gemini_mdl, text)
        except json.JSONDecodeError:
            await msg.edit_text("✖  לא הצלחתי לפענח. נסה ניסוח אחר.")
            return
        except Exception as exc:
            log.exception("Gemini text error")
            await msg.edit_text(f"✖  שגיאה: {str(exc)[:100]}")
            return

        await _process_event(uid, ev, msg.edit_text)

    # ── Image handler ──

    async def handle_image(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not get_cal_id(uid):
            await update.message.reply_text("שלח /start כדי להתחיל.")
            return

        msg = await update.message.reply_text("⠋ מנתח תמונה…")

        # Get the highest resolution photo
        photo = update.message.photo[-1]
        file = await ctx.bot.get_file(photo.file_id)
        buf = io.BytesIO()
        await file.download_to_memory(buf)
        image_bytes = buf.getvalue()

        caption = update.message.caption or ""

        try:
            ev = parse_image(gemini_mdl, image_bytes, caption)
        except json.JSONDecodeError:
            await msg.edit_text(
                "✖  לא הצלחתי לחלץ אירוע מהתמונה.\n"
                "נסה תמונה ברורה יותר או כתוב את הפרטים בטקסט."
            )
            return
        except Exception as exc:
            log.exception("Gemini image error")
            await msg.edit_text(f"✖  שגיאה: {str(exc)[:100]}")
            return

        await _process_event(uid, ev, msg.edit_text)

    # ── Document/file image handler (for uncompressed photos) ──

    async def handle_document_image(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        doc = update.message.document
        if not doc.mime_type or not doc.mime_type.startswith("image/"):
            return  # ignore non-image files

        uid = update.effective_user.id
        if not get_cal_id(uid):
            await update.message.reply_text("שלח /start כדי להתחיל.")
            return

        msg = await update.message.reply_text("⠋ מנתח תמונה…")

        file = await ctx.bot.get_file(doc.file_id)
        buf = io.BytesIO()
        await file.download_to_memory(buf)
        image_bytes = buf.getvalue()
        caption = update.message.caption or ""

        try:
            ev = parse_image(gemini_mdl, image_bytes, caption)
        except json.JSONDecodeError:
            await msg.edit_text("✖  לא הצלחתי לחלץ אירוע מהתמונה.")
            return
        except Exception as exc:
            log.exception("Gemini doc-image error")
            await msg.edit_text(f"✖  שגיאה: {str(exc)[:100]}")
            return

        await _process_event(uid, ev, msg.edit_text)

    # ── Error handler ──

    async def on_error(update: object, ctx: ContextTypes.DEFAULT_TYPE):
        log.error("Unhandled:", exc_info=ctx.error)

    # ── Build application ──

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(ConversationHandler(
        entry_points=[
            CommandHandler("start", cmd_start),
            CommandHandler("setup", cmd_setup),
        ],
        states={
            STATE_CAL_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_cal_id)],
        },
        fallbacks=[CommandHandler("help", cmd_help)],
    ))

    app.add_handler(CommandHandler("help",   cmd_help))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("undo",   cmd_undo))
    app.add_handler(CallbackQueryHandler(callback_undo, pattern="^undo$"))
    app.add_handler(MessageHandler(filters.PHOTO, handle_image))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document_image))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_error_handler(on_error)

    return app

# ═══════════════════════════════════════
#  Main
# ═══════════════════════════════════════

def main():
    for var in ("TELEGRAM_BOT_TOKEN", "GEMINI_API_KEY", "GOOGLE_SA_JSON_B64"):
        if not os.environ.get(var):
            sys.exit(f"Missing: {var}")

    cal_svc, sa_email = init_google()
    gemini_mdl = init_gemini()
    log.info("SA: %s | Model: %s", sa_email, GEMINI_MODEL)

    app = build_app(cal_svc, sa_email, gemini_mdl)
    log.info("Bot starting…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
