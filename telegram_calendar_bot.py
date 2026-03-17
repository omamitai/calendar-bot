#!/usr/bin/env python3
"""
Telegram Calendar Bot — Production
Hebrew free-text & screenshots → Google Calendar
"""

import json, logging, os, re, sys, base64, io, time, traceback
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    BotCommand,
)
from telegram.ext import (
    Application, CommandHandler, ConversationHandler,
    MessageHandler, CallbackQueryHandler, ContextTypes, filters,
)
from telegram.constants import ChatAction, ParseMode

# ═══════════════════════════════════════
#  Config
# ═══════════════════════════════════════

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
GEMINI_API_KEY     = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL       = "gemini-flash-lite-latest"
TZ                 = "Asia/Jerusalem"
SCOPES             = ["https://www.googleapis.com/auth/calendar"]
USERS_FILE         = Path("/tmp/users.json")
HISTORY_FILE       = Path("/tmp/event_history.json")
MAX_RETRIES        = 2

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

ST_CAL_ID = 0

# ═══════════════════════════════════════
#  Persistence
# ═══════════════════════════════════════

def _load(p: Path) -> dict:
    try:
        return json.loads(p.read_text("utf-8")) if p.exists() else {}
    except Exception:
        return {}

def _save(p: Path, d: dict):
    try:
        p.write_text(json.dumps(d, ensure_ascii=False, indent=2), "utf-8")
    except Exception as e:
        log.error("Save error %s: %s", p, e)

def get_cal_id(uid: int) -> str | None:
    return _load(USERS_FILE).get(str(uid))

def set_cal_id(uid: int, cid: str):
    d = _load(USERS_FILE); d[str(uid)] = cid; _save(USERS_FILE, d)

def save_event(uid: int, event_id: str, cal_id: str, data: dict):
    h = _load(HISTORY_FILE)
    k = str(uid)
    if k not in h: h[k] = []
    h[k].append({"eid": event_id, "cid": cal_id, "data": data,
                  "ts": datetime.now(ZoneInfo(TZ)).isoformat()})
    h[k] = h[k][-50:]
    _save(HISTORY_FILE, h)

def pop_event(uid: int) -> dict | None:
    h = _load(HISTORY_FILE)
    k = str(uid)
    lst = h.get(k, [])
    if not lst: return None
    ev = lst.pop()
    _save(HISTORY_FILE, h)
    return ev

def get_last_event(uid: int) -> dict | None:
    h = _load(HISTORY_FILE)
    lst = h.get(str(uid), [])
    return lst[-1] if lst else None

# ═══════════════════════════════════════
#  Google Calendar
# ═══════════════════════════════════════

def init_google():
    b64 = os.environ.get("GOOGLE_SA_JSON_B64", "")
    if not b64: sys.exit("GOOGLE_SA_JSON_B64 missing")
    p = Path("/tmp/sa.json")
    p.write_bytes(base64.b64decode(b64))
    creds = service_account.Credentials.from_service_account_file(str(p), scopes=SCOPES)
    svc = build("calendar", "v3", credentials=creds)
    email = json.loads(p.read_text()).get("client_email", "?")
    return svc, email


def gcal_insert(svc, cal_id: str, ev: dict) -> dict:
    tz = ZoneInfo(TZ)
    body = {"summary": ev["title"]}
    if ev.get("location"):    body["location"]    = ev["location"]
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
        body["start"] = {"dateTime": s.isoformat(), "timeZone": TZ}
        body["end"]   = {"dateTime": e.isoformat(), "timeZone": TZ}

    return svc.events().insert(calendarId=cal_id, body=body).execute()


def gcal_delete(svc, cal_id: str, eid: str):
    try:
        svc.events().delete(calendarId=cal_id, eventId=eid).execute()
    except Exception as e:
        log.warning("Delete failed (may already be gone): %s", e)

# ═══════════════════════════════════════
#  Gemini
# ═══════════════════════════════════════

TEXT_PROMPT = """\
Extract the calendar event from the Hebrew text below.

Today: {today}
Timezone: Asia/Jerusalem

Return ONLY a JSON object — no markdown fences, no explanation, no text before or after:

{{"title":"כותרת קצרה","date":"YYYY-MM-DD","start_time":"HH:MM or null","end_time":"HH:MM or null","location":"מיקום or null","description":"פרטים or null","is_all_day":true/false}}

Rules:
- ראשון=Sun שני=Mon שלישי=Tue רביעי=Wed חמישי=Thu שישי=Fri שבת=Sat
- מחר=tomorrow. בעוד שבוע=+7d. יום X הבא=next X.
- No time mentioned → is_all_day:true, start_time:null
- Keep title short and natural in Hebrew
- PURE JSON ONLY

Text: {input}"""

IMG_PROMPT = """\
Extract the calendar event from this image (screenshot, flyer, WhatsApp message, poster, invitation).
Find: dates, times, event names, locations.

Today: {today} | Timezone: Asia/Jerusalem

Return ONLY a JSON object — no markdown, no text:

{{"title":"כותרת קצרה","date":"YYYY-MM-DD","start_time":"HH:MM or null","end_time":"HH:MM or null","location":"מיקום or null","description":"פרטים or null","is_all_day":true/false}}

Hebrew title. Main event only. PURE JSON ONLY."""

DATE_PROMPT = """Today is {today}. Convert "{input}" to a date. Reply with ONLY YYYY-MM-DD, nothing else."""


def init_gemini():
    genai.configure(api_key=GEMINI_API_KEY)
    return genai.GenerativeModel(
        GEMINI_MODEL,
        generation_config={"temperature": 0.05, "max_output_tokens": 1024},
    )


def _extract_json(raw: str) -> dict:
    """Robust JSON extraction — handles fences, preamble, trailing text."""
    raw = raw.strip()
    # Try raw first
    try: return json.loads(raw)
    except Exception: pass
    # Strip markdown fences
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw)
    cleaned = re.sub(r"\s*```\s*$", "", cleaned)
    try: return json.loads(cleaned.strip())
    except Exception: pass
    # Find first { ... } block
    m = re.search(r"\{[^{}]*\}", raw, re.DOTALL)
    if m:
        try: return json.loads(m.group())
        except Exception: pass
    # Find nested { ... { ... } ... }
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if m:
        try: return json.loads(m.group())
        except Exception: pass
    raise ValueError(f"No JSON found in: {raw[:120]}")


def _gemini_call(model, content, retries=MAX_RETRIES):
    """Call Gemini with retry."""
    for attempt in range(retries + 1):
        try:
            r = model.generate_content(content)
            if r and r.text:
                return r.text
        except Exception as e:
            log.warning("Gemini attempt %d: %s", attempt + 1, e)
            if attempt < retries:
                time.sleep(1.5 * (attempt + 1))
    raise RuntimeError("Gemini failed after retries")


def llm_text(model, text: str) -> dict:
    today = datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%d (%A)")
    raw = _gemini_call(model, TEXT_PROMPT.format(today=today, input=text))
    return _extract_json(raw)


def llm_image(model, img_bytes: bytes, caption: str = "") -> dict:
    import PIL.Image
    today = datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%d (%A)")
    prompt = IMG_PROMPT.format(today=today)
    if caption: prompt += f"\n\nUser note: {caption}"
    img = PIL.Image.open(io.BytesIO(img_bytes))
    raw = _gemini_call(model, [prompt, img])
    return _extract_json(raw)


def llm_parse_date(model, text: str) -> str | None:
    today = datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%d (%A)")
    try:
        raw = _gemini_call(model, DATE_PROMPT.format(today=today, input=text))
        raw = raw.strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
            datetime.strptime(raw, "%Y-%m-%d")  # validate
            return raw
    except Exception:
        pass
    return None

# ═══════════════════════════════════════
#  Validation
# ═══════════════════════════════════════

def validate_event(ev: dict) -> str | None:
    """Returns error string or None if valid."""
    if not ev.get("title"):
        return "חסרה כותרת לאירוע"
    if not ev.get("date"):
        return "חסר תאריך"
    try:
        datetime.strptime(ev["date"], "%Y-%m-%d")
    except (ValueError, TypeError):
        return "תאריך לא תקין"
    if ev.get("start_time"):
        if not re.match(r"^\d{1,2}:\d{2}$", ev["start_time"]):
            return "שעת התחלה לא תקינה"
    if ev.get("end_time"):
        if not re.match(r"^\d{1,2}:\d{2}$", ev["end_time"]):
            ev["end_time"] = None  # silently fix
    return None

# ═══════════════════════════════════════
#  Formatting
# ═══════════════════════════════════════

R = "\u200F"

_DAYS = {
    "Sunday": "ראשון", "Monday": "שני", "Tuesday": "שלישי",
    "Wednesday": "רביעי", "Thursday": "חמישי",
    "Friday": "שישי", "Saturday": "שבת",
}
_MONTHS = {
    1: "ינואר", 2: "פברואר", 3: "מרץ", 4: "אפריל",
    5: "מאי", 6: "יוני", 7: "יולי", 8: "אוגוסט",
    9: "ספטמבר", 10: "אוקטובר", 11: "נובמבר", 12: "דצמבר",
}


def _he_date(ds: str) -> str:
    d = datetime.strptime(ds, "%Y-%m-%d")
    return f"יום {_DAYS.get(d.strftime('%A'), '')}, {d.day} ב{_MONTHS.get(d.month, '')} {d.year}"


def _he_time(ev: dict) -> str:
    if ev.get("is_all_day") or not ev.get("start_time"):
        return "כל היום"
    s = ev["start_time"]
    if ev.get("end_time"):
        return f"{s} – {ev['end_time']}"
    return s


def _relative_date(ds: str) -> str:
    """Returns 'מחר', 'היום', or '' for context."""
    try:
        d = datetime.strptime(ds, "%Y-%m-%d").date()
        today = datetime.now(ZoneInfo(TZ)).date()
        if d == today: return " (היום)"
        if d == today + timedelta(days=1): return " (מחר)"
        if d == today + timedelta(days=2): return " (מחרתיים)"
    except Exception:
        pass
    return ""


def card_confirmed(ev: dict, link: str = "") -> str:
    """Event created — clean confirmation."""
    rel = _relative_date(ev["date"])
    lines = [
        f"{R}✅  <b>נוסף ליומן</b>",
        "",
        f"{R}     <b>{ev['title']}</b>",
        f"{R}     ─────────────────",
        f"{R}     📅  {_he_date(ev['date'])}{rel}",
        f"{R}     🕐  {_he_time(ev)}",
    ]
    if ev.get("location"):
        lines.append(f"{R}     📍  {ev['location']}")
    if ev.get("description"):
        lines.append(f"{R}     📝  {ev['description']}")
    if link:
        lines += ["", f"{R}     <a href=\"{link}\">פתח ביומן ←</a>"]
    return "\n".join(lines)


def card_editing(ev: dict) -> str:
    """Preview card during edit — shows what will be saved."""
    rel = _relative_date(ev["date"])
    lines = [
        f"{R}✏️  <b>עריכת אירוע</b>",
        "",
        f"{R}     <b>{ev['title']}</b>",
        f"{R}     ─────────────────",
        f"{R}     📅  {_he_date(ev['date'])}{rel}",
        f"{R}     🕐  {_he_time(ev)}",
    ]
    if ev.get("location"):
        lines.append(f"{R}     📍  {ev['location']}")
    if ev.get("description"):
        lines.append(f"{R}     📝  {ev['description']}")
    lines += ["", f"{R}<i>לחץ על שדה לעריכה, או אשר:</i>"]
    return "\n".join(lines)


def card_cancelled(title: str) -> str:
    return f"{R}🗑  <s>{title}</s>  ·  בוטל"


def card_error(msg: str) -> str:
    return f"{R}⚠️  {msg}"

# ═══════════════════════════════════════
#  Keyboards
# ═══════════════════════════════════════

def kb_post_create() -> InlineKeyboardMarkup:
    """After event is created — undo + edit."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"✏️ ערוך", callback_data="act:edit"),
            InlineKeyboardButton(f"🗑 בטל", callback_data="act:undo"),
        ],
    ])


def kb_edit(ev: dict) -> InlineKeyboardMarkup:
    """Edit mode — field buttons + save."""
    buttons = [
        [
            InlineKeyboardButton(f"שם", callback_data="ed:title"),
            InlineKeyboardButton(f"תאריך", callback_data="ed:date"),
            InlineKeyboardButton(f"שעה", callback_data="ed:time"),
        ],
    ]
    if ev.get("location"):
        buttons[0].append(InlineKeyboardButton("מיקום", callback_data="ed:location"))
    buttons.append([
        InlineKeyboardButton(f"✅ שמור שינויים", callback_data="ed:save"),
        InlineKeyboardButton(f"↩️ בטל הכל", callback_data="act:undo"),
    ])
    return buttons_to_markup(buttons)


def buttons_to_markup(buttons):
    return InlineKeyboardMarkup(buttons)


BOT_COMMANDS = [
    BotCommand("start",  "חיבור ליומן"),
    BotCommand("undo",   "ביטול אירוע אחרון"),
    BotCommand("setup",  "שינוי יומן"),
    BotCommand("help",   "עזרה"),
]

# ═══════════════════════════════════════
#  Handlers
# ═══════════════════════════════════════

def build_app(cal_svc, sa_email, gemini_mdl):

    # ── Onboarding ──

    async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        uid = update.effective_user.id
        name = update.effective_user.first_name or ""

        if get_cal_id(uid):
            await update.message.reply_text(
                f"{R}👋  <b>{name}</b>, הבוט מוכן.\n\n"
                f"{R}שלח הודעה או תמונה — ואני מוסיף ליומן.\n"
                f"{R}לשינוי יומן — /setup  ·  לעזרה — /help",
                parse_mode=ParseMode.HTML,
            )
            return ConversationHandler.END

        await update.message.reply_text(
            f"{R}👋  <b>שלום {name}!</b>\n\n"
            f"{R}אני מוסיף אירועים ל-Google Calendar.\n"
            f"{R}כתוב בעברית או שלח תמונה — אני מטפל בשאר.\n\n"
            f"─────────────────────────\n\n"
            f"{R}<b>הגדרה חד-פעמית:</b>\n\n"
            f"{R}<b>①</b>  שתף את היומן שלך עם:\n"
            f"<code>{sa_email}</code>\n\n"
            f"{R}<i>Google Calendar → ⚙️ ליד היומן</i>\n"
            f"{R}<i>→ שיתוף עם אנשים ספציפיים</i>\n"
            f"{R}<i>→ הרשאה: ביצוע שינויים באירועים</i>\n\n"
            f"{R}<b>②</b>  שלח לי את כתובת ה-Gmail:",
            parse_mode=ParseMode.HTML,
        )
        return ST_CAL_ID

    async def cmd_setup(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        await update.message.reply_text(
            f"{R}⚙️  <b>החלפת יומן</b>\n\n"
            f"{R}ודא שיתוף עם:\n<code>{sa_email}</code>\n\n"
            f"{R}שלח כתובת Gmail:",
            parse_mode=ParseMode.HTML,
        )
        return ST_CAL_ID

    async def recv_cal_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        text = update.message.text.strip()
        if "@" not in text:
            await update.message.reply_text(
                f"{R}נסה כתובת Gmail, למשל:\n<code>name@gmail.com</code>",
                parse_mode=ParseMode.HTML,
            )
            return ST_CAL_ID

        await ctx.bot.send_chat_action(update.effective_chat.id, ChatAction.TYPING)

        try:
            cal_svc.calendarList().insert(body={"id": text}).execute()
        except Exception:
            pass

        try:
            cal_svc.events().list(calendarId=text, maxResults=1, singleEvents=True).execute()
        except Exception as exc:
            await update.message.reply_text(
                f"{R}⚠️  <b>אין גישה ליומן.</b>\n\n"
                f"{R}ודא:\n"
                f"{R}  · שיתפת עם <code>{sa_email}</code>\n"
                f"{R}  · הרשאת ביצוע שינויים\n\n"
                f"<code>{str(exc)[:90]}</code>",
                parse_mode=ParseMode.HTML,
            )
            return ST_CAL_ID

        set_cal_id(update.effective_user.id, text)
        await update.message.reply_text(
            f"{R}✅  <b>מחובר!</b>  <code>{text}</code>\n\n"
            f"{R}שלח אירוע בעברית או צילום מסך.",
            parse_mode=ParseMode.HTML,
        )
        return ConversationHandler.END

    # ── Help ──

    async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            f"{R}<b>עזרה</b>\n\n"

            f"{R}<b>✍️  טקסט</b>\n"
            f"{R}<i>\"פגישה עם דני מחר ב-10 בקפה ביאליק\"</i>\n"
            f"{R}<i>\"יום הולדת יעל 25 אפריל\"</i>\n"
            f"{R}<i>\"הרצאה יום חמישי 14:00–16:00\"</i>\n\n"

            f"{R}<b>📸  תמונה</b>\n"
            f"{R}צילום מסך, פלייר, הודעת WhatsApp\n\n"

            f"{R}<b>✏️  עריכה</b>\n"
            f"{R}אחרי הוספה — לחץ ✏️ לשנות פרטים\n\n"

            f"{R}<b>🗑  ביטול</b>\n"
            f"{R}כפתור מתחת לאירוע, או /undo\n\n"

            f"─────────────────────────\n"
            f"{R}/start · /setup · /undo · /help",
            parse_mode=ParseMode.HTML,
        )

    # ── Undo ──

    async def cmd_undo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        ev = pop_event(uid)
        if not ev:
            await update.message.reply_text(f"{R}אין מה לבטל.")
            return
        gcal_delete(cal_svc, ev["cid"], ev["eid"])
        await update.message.reply_text(
            card_cancelled(ev["data"].get("title", "?")),
            parse_mode=ParseMode.HTML,
        )

    # ── Create event (auto-add) ──

    async def _create_and_confirm(uid: int, ev: dict, reply_fn, edit_fn=None):
        """
        Create event in Google Calendar immediately, then show confirmation.
        reply_fn: for new messages.  edit_fn: for editing existing message.
        """
        cal_id = get_cal_id(uid)
        if not cal_id:
            fn = edit_fn or reply_fn
            await fn(f"{R}שלח /start כדי לחבר את היומן.")
            return

        err = validate_event(ev)
        if err:
            fn = edit_fn or reply_fn
            await fn(card_error(err), parse_mode=ParseMode.HTML)
            return

        try:
            created = gcal_insert(cal_svc, cal_id, ev)
        except Exception as exc:
            log.exception("Calendar insert error")
            fn = edit_fn or reply_fn
            await fn(card_error(f"שגיאה ביומן: {str(exc)[:80]}"), parse_mode=ParseMode.HTML)
            return

        eid = created.get("id", "")
        link = created.get("htmlLink", "")
        save_event(uid, eid, cal_id, ev)

        text = card_confirmed(ev, link)
        kb = kb_post_create()

        if edit_fn:
            await edit_fn(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        else:
            await reply_fn(text, parse_mode=ParseMode.HTML, reply_markup=kb)

    # ── Callback: undo ──

    async def cb_undo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        uid = q.from_user.id

        # If in edit mode — cancel edits AND delete the original event
        pending = ctx.user_data.pop("editing_event", None)
        orig_eid = ctx.user_data.pop("editing_original_eid", None)
        orig_cid = ctx.user_data.pop("editing_original_cid", None)
        ctx.user_data.pop("edit_msg_id", None)
        ctx.user_data.pop("edit_field", None)
        if pending:
            if orig_eid and orig_cid:
                gcal_delete(cal_svc, orig_cid, orig_eid)
                pop_event(uid)
            title = pending.get("title", "?")
            await q.edit_message_text(card_cancelled(title), parse_mode=ParseMode.HTML)
            return

        ev = pop_event(uid)
        if not ev:
            await q.edit_message_text(f"{R}אין מה לבטל.", parse_mode=ParseMode.HTML)
            return
        gcal_delete(cal_svc, ev["cid"], ev["eid"])
        await q.edit_message_text(
            card_cancelled(ev["data"].get("title", "?")),
            parse_mode=ParseMode.HTML,
        )

    # ── Callback: enter edit mode ──

    async def cb_enter_edit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        uid = q.from_user.id

        last = get_last_event(uid)
        if not last:
            await q.edit_message_text(f"{R}אין אירוע לעריכה.", parse_mode=ParseMode.HTML)
            return

        ev = dict(last["data"])  # copy
        ctx.user_data["editing_event"] = ev
        ctx.user_data["editing_original_eid"] = last["eid"]
        ctx.user_data["editing_original_cid"] = last["cid"]
        ctx.user_data["edit_msg_id"] = q.message.message_id

        await q.edit_message_text(
            card_editing(ev),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_edit(ev),
        )

    # ── Callback: pick field to edit ──

    async def cb_edit_field(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        field = q.data.split(":")[1]
        ctx.user_data["edit_field"] = field

        hints = {
            "title":    "שלח שם חדש לאירוע:",
            "date":     "שלח תאריך חדש (למשל: מחר, 25.3, יום חמישי):",
            "time":     "שלח שעה חדשה (למשל: 10:00 או 14:00-16:00):",
            "location": "שלח מיקום חדש:",
        }
        await q.message.reply_text(f"{R}✏️  {hints.get(field, 'שלח ערך חדש:')}")

    # ── Callback: save edits ──

    async def cb_save_edit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        uid = q.from_user.id

        ev = ctx.user_data.pop("editing_event", None)
        orig_eid = ctx.user_data.pop("editing_original_eid", None)
        orig_cid = ctx.user_data.pop("editing_original_cid", None)
        ctx.user_data.pop("edit_msg_id", None)

        if not ev:
            await q.edit_message_text(f"{R}אין שינויים לשמור.", parse_mode=ParseMode.HTML)
            return

        # Delete old event
        if orig_eid and orig_cid:
            gcal_delete(cal_svc, orig_cid, orig_eid)
            # Also remove from history
            pop_event(uid)

        # Create updated event
        await _create_and_confirm(uid, ev, q.message.reply_text, q.edit_message_text)

    # ── Handle text input during edit ──

    async def _try_handle_edit(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> bool:
        """If in edit mode, apply the field change. Returns True if handled."""
        field = ctx.user_data.pop("edit_field", None)
        ev = ctx.user_data.get("editing_event")
        if not field or not ev:
            return False

        text = update.message.text.strip()

        if field == "title":
            ev["title"] = text

        elif field == "location":
            ev["location"] = text

        elif field == "time":
            parts = re.split(r"[-–—]", text)
            t = parts[0].strip().replace(".", ":")
            # Normalize single-digit hour: "9:00" → "09:00"
            m = re.match(r"^(\d{1,2}):(\d{2})$", t)
            if m:
                ev["start_time"] = f"{int(m.group(1)):02d}:{m.group(2)}"
                ev["is_all_day"] = False
                if len(parts) > 1:
                    t2 = parts[1].strip().replace(".", ":")
                    m2 = re.match(r"^(\d{1,2}):(\d{2})$", t2)
                    if m2:
                        ev["end_time"] = f"{int(m2.group(1)):02d}:{m2.group(2)}"
                    else:
                        ev["end_time"] = None
                else:
                    ev["end_time"] = None
            else:
                await update.message.reply_text(
                    f"{R}פורמט לא תקין. נסה: <code>10:00</code> או <code>14:00-16:00</code>",
                    parse_mode=ParseMode.HTML,
                )
                ctx.user_data["edit_field"] = field  # keep in edit mode
                return True

        elif field == "date":
            # Try DD.MM or DD.MM.YYYY
            dm = re.match(r"(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?", text)
            if dm:
                day, month = int(dm.group(1)), int(dm.group(2))
                year = int(dm.group(3)) if dm.group(3) else datetime.now(ZoneInfo(TZ)).year
                if year < 100: year += 2000
                ev["date"] = f"{year}-{month:02d}-{day:02d}"
            else:
                parsed = llm_parse_date(gemini_mdl, text)
                if parsed:
                    ev["date"] = parsed
                else:
                    await update.message.reply_text(
                        f"{R}לא הבנתי את התאריך. נסה: <code>25.3</code> או <code>מחר</code>",
                        parse_mode=ParseMode.HTML,
                    )
                    ctx.user_data["edit_field"] = field
                    return True

        ctx.user_data["editing_event"] = ev

        # Update the edit card
        try:
            msg_id = ctx.user_data.get("edit_msg_id")
            if msg_id:
                await ctx.bot.edit_message_text(
                    chat_id=update.effective_chat.id,
                    message_id=msg_id,
                    text=card_editing(ev),
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_edit(ev),
                )
        except Exception as e:
            log.warning("Could not update edit card: %s", e)

        await update.message.reply_text(f"{R}✓ עודכן.")
        return True

    # ── Text handler ──

    async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        # Edit mode takes priority
        if await _try_handle_edit(update, ctx):
            return

        text = update.message.text.strip()
        if not text:
            return

        uid = update.effective_user.id
        if not get_cal_id(uid):
            await update.message.reply_text(
                f"{R}שלח /start כדי לחבר את היומן."
            )
            return

        await ctx.bot.send_chat_action(update.effective_chat.id, ChatAction.TYPING)

        try:
            ev = llm_text(gemini_mdl, text)
        except Exception as exc:
            log.exception("LLM text error")
            await update.message.reply_text(
                f"{R}לא הצלחתי לפענח. נסה ניסוח אחר:\n"
                f"<i>\"פגישה עם דני מחר ב-10\"</i>",
                parse_mode=ParseMode.HTML,
            )
            return

        # Auto-add to calendar
        await _create_and_confirm(uid, ev, update.message.reply_text)

    # ── Image handler ──

    async def _handle_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE, img_bytes: bytes, caption: str):
        uid = update.effective_user.id
        if not get_cal_id(uid):
            await update.message.reply_text(f"{R}שלח /start קודם.")
            return

        await ctx.bot.send_chat_action(update.effective_chat.id, ChatAction.TYPING)

        try:
            ev = llm_image(gemini_mdl, img_bytes, caption)
        except Exception as exc:
            log.exception("LLM image error")
            await update.message.reply_text(
                f"{R}לא הצלחתי לזהות אירוע בתמונה.\n"
                f"{R}נסה תמונה ברורה יותר, או כתוב בטקסט.",
            )
            return

        await _create_and_confirm(uid, ev, update.message.reply_text)

    async def handle_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        photo = update.message.photo[-1]
        file = await ctx.bot.get_file(photo.file_id)
        buf = io.BytesIO()
        await file.download_to_memory(buf)
        await _handle_photo(update, ctx, buf.getvalue(), update.message.caption or "")

    async def handle_doc_image(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        doc = update.message.document
        if not doc.mime_type or not doc.mime_type.startswith("image/"):
            return
        file = await ctx.bot.get_file(doc.file_id)
        buf = io.BytesIO()
        await file.download_to_memory(buf)
        await _handle_photo(update, ctx, buf.getvalue(), update.message.caption or "")

    # ── Error ──

    async def on_error(update: object, ctx: ContextTypes.DEFAULT_TYPE):
        log.error("Unhandled: %s", traceback.format_exception(ctx.error))

    # ── Post-init ──

    async def post_init(application: Application):
        await application.bot.set_my_commands(BOT_COMMANDS)
        log.info("Commands menu set")

    # ── Build ──

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()

    app.add_handler(ConversationHandler(
        entry_points=[
            CommandHandler("start", cmd_start),
            CommandHandler("setup", cmd_setup),
        ],
        states={
            ST_CAL_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_cal_id)],
        },
        fallbacks=[CommandHandler("help", cmd_help)],
    ))

    app.add_handler(CommandHandler("help",  cmd_help))
    app.add_handler(CommandHandler("undo",  cmd_undo))

    app.add_handler(CallbackQueryHandler(cb_undo,       pattern="^act:undo$"))
    app.add_handler(CallbackQueryHandler(cb_enter_edit,  pattern="^act:edit$"))
    app.add_handler(CallbackQueryHandler(cb_edit_field,  pattern="^ed:(title|date|time|location)$"))
    app.add_handler(CallbackQueryHandler(cb_save_edit,   pattern="^ed:save$"))

    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_doc_image))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    app.add_error_handler(on_error)
    return app

# ═══════════════════════════════════════
#  Main
# ═══════════════════════════════════════

def main():
    for v in ("TELEGRAM_BOT_TOKEN", "GEMINI_API_KEY", "GOOGLE_SA_JSON_B64"):
        if not os.environ.get(v): sys.exit(f"Missing: {v}")

    cal_svc, sa_email = init_google()
    gemini_mdl = init_gemini()
    log.info("SA: %s | Model: %s", sa_email, GEMINI_MODEL)

    app = build_app(cal_svc, sa_email, gemini_mdl)
    log.info("Bot starting…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
