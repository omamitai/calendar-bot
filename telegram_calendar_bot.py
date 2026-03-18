#!/usr/bin/env python3
"""
Telegram Calendar Bot — v2
Hebrew text & images → Google Calendar
PostgreSQL persistence · Reliable for 100+ users
"""

import asyncio
import json, logging, os, re, sys, base64, io, time, traceback
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from zoneinfo import ZoneInfo

import PIL.Image
import google.generativeai as genai
import psycopg2
import psycopg2.pool
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand,
)
from telegram.ext import (
    Application, CommandHandler, ConversationHandler,
    MessageHandler, CallbackQueryHandler, ContextTypes, filters,
)
from telegram.constants import ChatAction, ParseMode
from telegram.error import BadRequest, TimedOut, NetworkError

# ═══════════════════════════════════════
#  Config
# ═══════════════════════════════════════

TZ = "Asia/Jerusalem"
GEMINI_MODEL = "gemini-2.0-flash-lite"
SCOPES = ["https://www.googleapis.com/auth/calendar"]
LLM_TIMEOUT = 15           # seconds
LLM_RETRIES = 2
GEMINI_TEMP = 0.05
RATE_LIMIT = 30            # max requests per user per minute

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("calbot")

ST_CAL = 0
R = "\u200F"               # RTL mark

# ═══════════════════════════════════════
#  Database (PostgreSQL)
# ═══════════════════════════════════════

db_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def init_db():
    global db_pool
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        sys.exit("DATABASE_URL missing")
    # Railway uses postgres:// but psycopg2 needs postgresql://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    db_pool = psycopg2.pool.ThreadedConnectionPool(2, 20, url)
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    uid         BIGINT PRIMARY KEY,
                    calendar_id TEXT NOT NULL,
                    first_name  TEXT,
                    created_at  TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS history (
                    id          SERIAL PRIMARY KEY,
                    uid         BIGINT NOT NULL,
                    event_id    TEXT NOT NULL,
                    calendar_id TEXT NOT NULL,
                    data        JSONB NOT NULL,
                    created_at  TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_history_uid ON history(uid)"
            )
        conn.commit()
        log.info("Database ready")
    finally:
        db_pool.putconn(conn)


def get_cal(uid: int) -> str | None:
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT calendar_id FROM users WHERE uid = %s", (uid,))
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        db_pool.putconn(conn)


def set_cal(uid: int, cid: str, name: str = ""):
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (uid, calendar_id, first_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (uid) DO UPDATE
                SET calendar_id = EXCLUDED.calendar_id,
                    first_name  = EXCLUDED.first_name
            """, (uid, cid, name))
        conn.commit()
    except Exception as e:
        log.error("set_cal: %s", e)
        conn.rollback()
    finally:
        db_pool.putconn(conn)


def push_hist(uid: int, eid: str, cid: str, data: dict):
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO history (uid, event_id, calendar_id, data) "
                "VALUES (%s, %s, %s, %s)",
                (uid, eid, cid, json.dumps(data, ensure_ascii=False)),
            )
            # keep last 50 per user
            cur.execute("""
                DELETE FROM history
                WHERE uid = %s AND id NOT IN (
                    SELECT id FROM history WHERE uid = %s
                    ORDER BY id DESC LIMIT 50
                )
            """, (uid, uid))
        conn.commit()
    except Exception as e:
        log.error("push_hist: %s", e)
        conn.rollback()
    finally:
        db_pool.putconn(conn)


def pop_hist(uid: int) -> dict | None:
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM history WHERE id = (
                    SELECT id FROM history WHERE uid = %s
                    ORDER BY id DESC LIMIT 1
                ) RETURNING event_id, calendar_id, data
            """, (uid,))
            row = cur.fetchone()
        conn.commit()
        if not row:
            return None
        d = row[2] if isinstance(row[2], dict) else json.loads(row[2])
        return {"eid": row[0], "cid": row[1], "data": d}
    except Exception as e:
        log.error("pop_hist: %s", e)
        conn.rollback()
        return None
    finally:
        db_pool.putconn(conn)


def user_count() -> int:
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM users")
            return cur.fetchone()[0]
    finally:
        db_pool.putconn(conn)


# ═══════════════════════════════════════
#  Rate Limiting (in-memory, per-process)
# ═══════════════════════════════════════

_rate: dict[int, list[float]] = {}


def check_rate(uid: int) -> bool:
    now = time.time()
    times = _rate.get(uid, [])
    times = [t for t in times if now - t < 60]
    if len(times) >= RATE_LIMIT:
        return False
    times.append(now)
    _rate[uid] = times
    return True


# ═══════════════════════════════════════
#  Google Calendar
# ═══════════════════════════════════════

def init_google():
    b64 = os.environ.get("GOOGLE_SA_JSON_B64", "")
    if not b64:
        sys.exit("GOOGLE_SA_JSON_B64 missing")
    p = Path("/tmp/sa.json")
    p.write_bytes(base64.b64decode(b64))
    creds = service_account.Credentials.from_service_account_file(
        str(p), scopes=SCOPES
    )
    svc = build("calendar", "v3", credentials=creds, cache_discovery=False)
    email = json.loads(p.read_text()).get("client_email", "?")
    return svc, email


def gcal_insert(svc, cid, ev):
    tz = ZoneInfo(TZ)
    body = {"summary": ev["title"]}
    if ev.get("location"):
        body["location"] = ev["location"]
    if ev.get("description"):
        body["description"] = ev["description"]

    if ev.get("is_all_day") or not ev.get("start_time"):
        body["start"] = {"date": ev["date"]}
        end_date = ev.get("end_date", ev["date"])
        end = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        body["end"] = {"date": end.strftime("%Y-%m-%d")}
    else:
        s = datetime.strptime(
            f"{ev['date']} {ev['start_time']}", "%Y-%m-%d %H:%M"
        ).replace(tzinfo=tz)
        e = s + timedelta(hours=1)
        if ev.get("end_time"):
            e = datetime.strptime(
                f"{ev['date']} {ev['end_time']}", "%Y-%m-%d %H:%M"
            ).replace(tzinfo=tz)
            if e <= s:
                e += timedelta(days=1)
        body["start"] = {"dateTime": s.isoformat(), "timeZone": TZ}
        body["end"] = {"dateTime": e.isoformat(), "timeZone": TZ}

    return svc.events().insert(calendarId=cid, body=body).execute()


def gcal_del(svc, cid, eid):
    try:
        svc.events().delete(calendarId=cid, eventId=eid).execute()
    except HttpError as e:
        if e.resp.status != 410:  # 410 = already deleted
            log.warning("gcal_del: %s", e)
    except Exception as e:
        log.warning("gcal_del: %s", e)


# ═══════════════════════════════════════
#  Gemini LLM
# ═══════════════════════════════════════

TEXT_P = """\
Extract the calendar event. Today: {today}. Timezone: Asia/Jerusalem.

Return ONLY valid JSON, nothing else:
{{"title":"כותרת קצרה","date":"YYYY-MM-DD","start_time":"HH:MM or null","end_time":"HH:MM or null","location":"מיקום or null","description":"הערות or null","is_all_day":true/false}}

Day names: ראשון=Sun שני=Mon שלישי=Tue רביעי=Wed חמישי=Thu שישי=Fri שבת=Sat
מחר=tomorrow, בעוד שבוע=+7d, יום X הבא=next X.
No time → is_all_day:true. Keep title short, Hebrew. ONLY JSON!

Input: {input}"""

IMG_P = """\
Extract the calendar event from this image (screenshot/flyer/WhatsApp/poster).
Today: {today}. Timezone: Asia/Jerusalem.

Return ONLY valid JSON:
{{"title":"כותרת קצרה","date":"YYYY-MM-DD","start_time":"HH:MM or null","end_time":"HH:MM or null","location":"מיקום or null","description":"הערות or null","is_all_day":true/false}}

Hebrew title. Main event only. ONLY JSON!"""

DATE_P = """Today is {today}. Convert "{text}" to YYYY-MM-DD. Reply ONLY with the date."""


def init_gemini():
    genai.configure(api_key=os.environ["GEMINI_API_KEY"])
    return genai.GenerativeModel(
        GEMINI_MODEL,
        generation_config={"temperature": GEMINI_TEMP, "max_output_tokens": 512},
    )


def _find_json(raw: str) -> dict:
    """Extract JSON from LLM output — tries 4 strategies."""
    raw = raw.strip()
    for text in [raw, re.sub(r"^```(?:json)?\s*|\s*```\s*$", "", raw)]:
        try:
            return json.loads(text.strip())
        except (json.JSONDecodeError, ValueError):
            pass
    for pattern in [r"\{[^{}]*\}", r"\{.*\}"]:
        m = re.search(pattern, raw, re.DOTALL)
        if m:
            try:
                return json.loads(m.group())
            except (json.JSONDecodeError, ValueError):
                pass
    raise ValueError(f"No JSON in: {raw[:120]}")


async def _call_gemini(model, content, timeout=LLM_TIMEOUT, retries=LLM_RETRIES):
    """Call Gemini in a thread with timeout + retry."""
    loop = asyncio.get_event_loop()
    for attempt in range(retries + 1):
        try:
            fn = partial(model.generate_content, content)
            r = await asyncio.wait_for(
                loop.run_in_executor(None, fn), timeout=timeout
            )
            if r and r.text:
                return r.text
        except asyncio.TimeoutError:
            log.warning("Gemini timeout (attempt %d/%d)", attempt + 1, retries + 1)
        except Exception as e:
            log.warning("Gemini error (attempt %d/%d): %s", attempt + 1, retries + 1, e)
        if attempt < retries:
            await asyncio.sleep(1.0 * (attempt + 1))
    raise RuntimeError("Gemini unavailable after retries")


async def llm_text(model, text):
    today = datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%d (%A)")
    raw = await _call_gemini(model, TEXT_P.format(today=today, input=text))
    return _find_json(raw)


async def llm_image(model, img_bytes, caption=""):
    today = datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%d (%A)")
    prompt = IMG_P.format(today=today)
    if caption:
        prompt += f"\nUser note: {caption}"
    img = PIL.Image.open(io.BytesIO(img_bytes))
    raw = await _call_gemini(model, [prompt, img], timeout=20)
    return _find_json(raw)


async def llm_date(model, text):
    today = datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%d (%A)")
    try:
        raw = await _call_gemini(model, DATE_P.format(today=today, text=text), timeout=8)
        d = raw.strip()[:10]
        if re.match(r"\d{4}-\d{2}-\d{2}$", d):
            datetime.strptime(d, "%Y-%m-%d")
            return d
    except Exception:
        pass
    return None


# ═══════════════════════════════════════
#  Validation & Normalization
# ═══════════════════════════════════════

def _norm_time(t: str | None) -> str | None:
    if not t:
        return None
    t = t.strip().replace(".", ":")
    m = re.match(r"^(\d{1,2}):(\d{2})$", t)
    if not m:
        return None
    h, mn = int(m.group(1)), int(m.group(2))
    if h > 23 or mn > 59:
        return None
    return f"{h:02d}:{mn:02d}"


def normalize(ev: dict) -> dict:
    """Clean and normalize event data."""
    ev["title"] = (ev.get("title") or "").strip()[:100]
    ev["date"] = (ev.get("date") or "").strip()[:10]
    ev["start_time"] = _norm_time(ev.get("start_time"))
    ev["end_time"] = _norm_time(ev.get("end_time"))
    ev["location"] = (ev.get("location") or "").strip()[:200] or None
    ev["description"] = (ev.get("description") or "").strip()[:500] or None
    if not ev["start_time"]:
        ev["is_all_day"] = True
        ev["end_time"] = None
    else:
        ev["is_all_day"] = False  # has a time → not all-day
    return ev


def validate(ev: dict) -> str | None:
    """Returns error message or None."""
    if not ev.get("title"):
        return "חסרה כותרת"
    if not ev.get("date"):
        return "חסר תאריך"
    try:
        datetime.strptime(ev["date"], "%Y-%m-%d")
    except (ValueError, TypeError):
        return "תאריך לא תקין"
    return None


# ═══════════════════════════════════════
#  Formatting
# ═══════════════════════════════════════

_DAY = {
    "Sunday": "ראשון", "Monday": "שני", "Tuesday": "שלישי",
    "Wednesday": "רביעי", "Thursday": "חמישי",
    "Friday": "שישי", "Saturday": "שבת",
}
_MON = {
    1: "ינואר", 2: "פברואר", 3: "מרץ", 4: "אפריל",
    5: "מאי", 6: "יוני", 7: "יולי", 8: "אוגוסט",
    9: "ספטמבר", 10: "אוקטובר", 11: "נובמבר", 12: "דצמבר",
}


def _hdate(ds):
    d = datetime.strptime(ds, "%Y-%m-%d")
    return f"יום {_DAY.get(d.strftime('%A'), '')}, {d.day} ב{_MON.get(d.month, '')} {d.year}"


def _htime(ev):
    if ev.get("is_all_day") or not ev.get("start_time"):
        return "כל היום"
    s = ev["start_time"]
    return f"{s} – {ev['end_time']}" if ev.get("end_time") else s


def _rel(ds):
    try:
        d = datetime.strptime(ds, "%Y-%m-%d").date()
        t = datetime.now(ZoneInfo(TZ)).date()
        if d == t:
            return "  · היום"
        if d == t + timedelta(1):
            return "  · מחר"
        if d == t + timedelta(2):
            return "  · מחרתיים"
        diff = (d - t).days
        if 0 < diff <= 7:
            return f"  · בעוד {diff} ימים"
    except Exception:
        pass
    return ""


def card_ok(ev, link=""):
    """Confirmed event card."""
    lines = [
        f"{R}✅  <b>{ev['title']}</b>",
        "",
        f"{R}📅  {_hdate(ev['date'])}{_rel(ev['date'])}",
        f"{R}🕐  {_htime(ev)}",
    ]
    if ev.get("location"):
        lines.append(f"{R}📍  {ev['location']}")
    if ev.get("description"):
        lines.append(f"{R}📝  {ev['description']}")
    if link:
        lines.append(f"\n{R}<a href=\"{link}\">פתח ביומן ←</a>")
    return "\n".join(lines)


def card_edit(ev):
    """Edit-mode card."""
    lines = [
        f"{R}✏️  <b>{ev['title']}</b>",
        "",
        f"{R}📅  {_hdate(ev['date'])}{_rel(ev['date'])}",
        f"{R}🕐  {_htime(ev)}",
    ]
    if ev.get("location"):
        lines.append(f"{R}📍  {ev['location']}")
    if ev.get("description"):
        lines.append(f"{R}📝  {ev['description']}")
    lines.append(f"\n{R}<i>בחר שדה לעריכה:</i>")
    return "\n".join(lines)


def card_del(title):
    return f"{R}<s>{title}</s>  ·  🗑 בוטל"


# ═══════════════════════════════════════
#  Keyboards
# ═══════════════════════════════════════

def kb_done():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✏️ ערוך", callback_data="a:edit"),
        InlineKeyboardButton("🗑 בטל", callback_data="a:del"),
    ]])


def kb_edit(ev):
    row1 = [
        InlineKeyboardButton("📌 שם", callback_data="e:title"),
        InlineKeyboardButton("📅 תאריך", callback_data="e:date"),
        InlineKeyboardButton("🕐 שעה", callback_data="e:time"),
    ]
    row2 = [
        InlineKeyboardButton(
            "📍 מיקום" if ev.get("location") else "➕ מיקום",
            callback_data="e:loc",
        ),
        InlineKeyboardButton(
            "📝 הערה" if ev.get("description") else "➕ הערה",
            callback_data="e:desc",
        ),
    ]
    row3 = [
        InlineKeyboardButton("✅ שמור", callback_data="e:save"),
        InlineKeyboardButton("↩️ ביטול", callback_data="a:del"),
    ]
    return InlineKeyboardMarkup([row1, row2, row3])


CMDS = [
    BotCommand("start", "חיבור ליומן"),
    BotCommand("undo", "ביטול אירוע אחרון"),
    BotCommand("setup", "שינוי יומן"),
    BotCommand("status", "מצב חיבור"),
    BotCommand("help", "עזרה"),
]

# ═══════════════════════════════════════
#  Safe message helpers
# ═══════════════════════════════════════


async def _safe_edit(msg, text, **kw):
    """Edit message text, ignoring 'message not modified' errors."""
    try:
        await msg.edit_text(text, **kw)
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            raise


async def _safe_edit_reply_markup(msg, markup):
    try:
        await msg.edit_reply_markup(reply_markup=markup)
    except BadRequest:
        pass


# ═══════════════════════════════════════
#  Application
# ═══════════════════════════════════════

def build_app(svc, sa_email, mdl):

    # ── Setup ──

    async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        uid = update.effective_user.id
        name = update.effective_user.first_name or ""
        if get_cal(uid):
            await update.message.reply_text(
                f"{R}👋  <b>{name}</b>, הבוט מוכן.\n"
                f"{R}שלח אירוע בטקסט או תמונה.\n\n"
                f"{R}/setup — שינוי יומן  ·  /help — עזרה",
                parse_mode=ParseMode.HTML,
            )
            return ConversationHandler.END
        await update.message.reply_text(
            f"{R}👋  <b>שלום{(' ' + name) if name else ''}!</b>\n\n"
            f"{R}אני בוט שמוסיף אירועים ל-Google Calendar.\n"
            f"{R}כתוב בעברית או שלח צילום מסך — ואני אטפל בשאר.\n\n"
            f"─────────────────────\n\n"
            f"{R}<b>🔧 הגדרה חד-פעמית:</b>\n\n"
            f"{R}<b>①</b>  שתף את היומן עם:\n"
            f"<code>{sa_email}</code>\n\n"
            f"{R}<i>Google Calendar → ⚙️ ליד היומן → שיתוף</i>\n"
            f"{R}<i>→ הרשאה: ביצוע שינויים באירועים</i>\n\n"
            f"{R}<b>②</b>  שלח את כתובת ה-Gmail שלך:",
            parse_mode=ParseMode.HTML,
        )
        return ST_CAL

    async def cmd_setup(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        await update.message.reply_text(
            f"{R}⚙️  <b>החלפת יומן</b>\n\n"
            f"{R}ודא שיתוף עם:\n<code>{sa_email}</code>\n\n"
            f"{R}שלח כתובת Gmail:",
            parse_mode=ParseMode.HTML,
        )
        return ST_CAL

    async def recv_cal(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        text = update.message.text.strip()
        if "@" not in text:
            await update.message.reply_text(
                f"{R}שלח כתובת Gmail, למשל:\n<code>name@gmail.com</code>",
                parse_mode=ParseMode.HTML,
            )
            return ST_CAL
        await ctx.bot.send_chat_action(update.effective_chat.id, ChatAction.TYPING)
        # Try subscribing the SA to the user's calendar
        try:
            svc.calendarList().insert(body={"id": text}).execute()
        except HttpError:
            pass  # Already subscribed or will verify below
        except Exception as e:
            log.warning("calendarList insert: %s", e)
        # Verify access
        try:
            svc.events().list(
                calendarId=text, maxResults=1, singleEvents=True
            ).execute()
        except Exception as exc:
            await update.message.reply_text(
                f"{R}⚠️  <b>אין גישה.</b>\n\n"
                f"{R}ודא שיתוף עם:\n<code>{sa_email}</code>\n"
                f"{R}+ הרשאת <b>ביצוע שינויים באירועים</b>\n\n"
                f"<code>{str(exc)[:80]}</code>",
                parse_mode=ParseMode.HTML,
            )
            return ST_CAL
        name = update.effective_user.first_name or ""
        set_cal(update.effective_user.id, text, name)
        await update.message.reply_text(
            f"{R}✅  <b>מחובר!</b>\n\n"
            f"{R}שלח אירוע בטקסט או תמונה ואני אוסיף אותו ליומן.",
            parse_mode=ParseMode.HTML,
        )
        return ConversationHandler.END

    # ── Help ──

    async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            f"{R}<b>📖 איך משתמשים?</b>\n\n"
            f"{R}<b>✍️  טקסט</b> — כתוב אירוע בשפה חופשית:\n"
            f'{R}<i>"פגישה עם דני מחר ב-10 בקפה"</i>\n'
            f'{R}<i>"הרצאה יום חמישי 14:00–16:00"</i>\n'
            f'{R}<i>"רופא שיניים בעוד שבועיים ב-9"</i>\n\n'
            f"{R}<b>📸  תמונה</b> — צילום מסך, פלייר, הודעת WhatsApp\n\n"
            f"{R}<b>✏️  עריכה</b> — לחץ ✏️ לאחר הוספה כדי לשנות פרטים\n"
            f"{R}<b>🗑  ביטול</b> — לחץ 🗑 או שלח /undo\n\n"
            f"─────────────────────\n"
            f"{R}/start · /setup · /undo · /status · /help",
            parse_mode=ParseMode.HTML,
        )

    # ── Status ──

    async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        cid = get_cal(uid)
        if cid:
            await update.message.reply_text(
                f"{R}✅  <b>מחובר</b>\n"
                f"{R}יומן: <code>{cid}</code>\n\n"
                f"{R}/setup כדי להחליף",
                parse_mode=ParseMode.HTML,
            )
        else:
            await update.message.reply_text(
                f"{R}❌  <b>לא מחובר</b>\n{R}שלח /start כדי להתחיל.",
                parse_mode=ParseMode.HTML,
            )

    # ── Undo ──

    async def cmd_undo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        ev = pop_hist(update.effective_user.id)
        if not ev:
            await update.message.reply_text(f"{R}אין מה לבטל.")
            return
        gcal_del(svc, ev["cid"], ev["eid"])
        await update.message.reply_text(
            card_del(ev["data"].get("title", "?")),
            parse_mode=ParseMode.HTML,
        )

    # ── Core flow: parse → create → show ──

    async def _process(uid, ev, msg):
        """Create event in calendar, update msg with result."""
        cid = get_cal(uid)
        if not cid:
            await _safe_edit(
                msg, f"{R}שלח /start כדי לחבר.", parse_mode=ParseMode.HTML
            )
            return
        ev = normalize(ev)
        err = validate(ev)
        if err:
            await _safe_edit(
                msg,
                f"{R}⚠️  {err}\n{R}נסה שוב בניסוח אחר.",
                parse_mode=ParseMode.HTML,
            )
            return
        try:
            created = gcal_insert(svc, cid, ev)
        except Exception as exc:
            log.exception("Calendar error for user %s", uid)
            await _safe_edit(
                msg,
                f"{R}⚠️  שגיאה ביומן:\n<code>{str(exc)[:80]}</code>",
                parse_mode=ParseMode.HTML,
            )
            return
        push_hist(uid, created["id"], cid, ev)
        await _safe_edit(
            msg,
            card_ok(ev, created.get("htmlLink", "")),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_done(),
        )

    # ── Text ──

    async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        # Edit mode?
        if ctx.user_data.get("edit_field"):
            await _apply_edit(update, ctx)
            return

        text = update.message.text.strip()
        if not text:
            return
        uid = update.effective_user.id
        if not get_cal(uid):
            await update.message.reply_text(f"{R}שלח /start כדי להתחיל.")
            return
        if not check_rate(uid):
            await update.message.reply_text(f"{R}⏳  לאט לאט... נסה שוב עוד רגע.")
            return

        msg = await update.message.reply_text(f"{R}⏳  מעבד...")
        await ctx.bot.send_chat_action(update.effective_chat.id, ChatAction.TYPING)

        try:
            ev = await llm_text(mdl, text)
        except Exception:
            log.exception("LLM error for user %s", uid)
            await _safe_edit(
                msg,
                f'{R}לא הצלחתי לפענח.\n{R}נסה למשל: <i>"פגישה עם דני מחר ב-10"</i>',
                parse_mode=ParseMode.HTML,
            )
            return

        await _process(uid, ev, msg)

    # ── Image ──

    async def _handle_img(update, ctx, img_bytes, caption):
        uid = update.effective_user.id
        if not get_cal(uid):
            await update.message.reply_text(f"{R}שלח /start קודם.")
            return
        if not check_rate(uid):
            await update.message.reply_text(f"{R}⏳  לאט לאט... נסה שוב עוד רגע.")
            return

        msg = await update.message.reply_text(f"{R}⏳  מזהה אירוע בתמונה...")
        await ctx.bot.send_chat_action(update.effective_chat.id, ChatAction.TYPING)
        try:
            ev = await llm_image(mdl, img_bytes, caption)
        except Exception:
            log.exception("LLM image error for user %s", uid)
            await _safe_edit(
                msg,
                f"{R}לא זיהיתי אירוע בתמונה.\n{R}נסה תמונה ברורה יותר או כתוב בטקסט.",
            )
            return
        await _process(uid, ev, msg)

    async def handle_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        f = await ctx.bot.get_file(update.message.photo[-1].file_id)
        buf = io.BytesIO()
        await f.download_to_memory(buf)
        await _handle_img(update, ctx, buf.getvalue(), update.message.caption or "")

    async def handle_doc(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        d = update.message.document
        if not d.mime_type or not d.mime_type.startswith("image/"):
            return
        f = await ctx.bot.get_file(d.file_id)
        buf = io.BytesIO()
        await f.download_to_memory(buf)
        await _handle_img(update, ctx, buf.getvalue(), update.message.caption or "")

    # ── Callbacks ──

    async def cb_del(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        uid = q.from_user.id

        # Cancel edit if active
        ed = ctx.user_data.pop("editing", None)
        ctx.user_data.pop("edit_field", None)
        ctx.user_data.pop("edit_msg_id", None)
        orig = ctx.user_data.pop("edit_orig", None)

        if ed:
            if orig:
                gcal_del(svc, orig["cid"], orig["eid"])
                pop_hist(uid)
            await q.edit_message_text(
                card_del(ed.get("title", "?")), parse_mode=ParseMode.HTML
            )
            return

        ev = pop_hist(uid)
        if not ev:
            await _safe_edit(
                q.message, f"{R}אין מה לבטל.", parse_mode=ParseMode.HTML
            )
            return
        gcal_del(svc, ev["cid"], ev["eid"])
        await q.edit_message_text(
            card_del(ev["data"].get("title", "?")), parse_mode=ParseMode.HTML
        )

    async def cb_edit_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        uid = q.from_user.id

        # Get last event from DB
        conn = db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT event_id, calendar_id, data FROM history "
                    "WHERE uid = %s ORDER BY id DESC LIMIT 1",
                    (uid,),
                )
                row = cur.fetchone()
        finally:
            db_pool.putconn(conn)

        if not row:
            await _safe_edit(
                q.message, f"{R}אין אירוע לעריכה.", parse_mode=ParseMode.HTML
            )
            return
        d = row[2] if isinstance(row[2], dict) else json.loads(row[2])
        ev = dict(d)
        ctx.user_data["editing"] = ev
        ctx.user_data["edit_orig"] = {"eid": row[0], "cid": row[1]}
        ctx.user_data["edit_msg_id"] = q.message.message_id
        await q.edit_message_text(
            card_edit(ev), parse_mode=ParseMode.HTML, reply_markup=kb_edit(ev)
        )

    async def cb_edit_field(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        field = q.data.split(":")[1]
        ctx.user_data["edit_field"] = field
        hints = {
            "title": "📌  שלח שם חדש:",
            "date": "📅  שלח תאריך (25.3 / מחר / יום חמישי):",
            "time": "🕐  שלח שעה (10:00 / 14:00-16:00):",
            "loc": "📍  שלח מיקום:",
            "desc": "📝  שלח הערה:",
        }
        await q.message.reply_text(f"{R}{hints.get(field, 'שלח ערך:')}")

    async def _apply_edit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        field = ctx.user_data.pop("edit_field", None)
        ev = ctx.user_data.get("editing")
        if not field or not ev:
            return

        text = update.message.text.strip()
        ok = True

        if field == "title":
            ev["title"] = text[:100]
        elif field == "loc":
            ev["location"] = text[:200]
        elif field == "desc":
            ev["description"] = text[:500]
        elif field == "time":
            parts = re.split(r"[-–—]", text)
            t = _norm_time(parts[0])
            if t:
                ev["start_time"] = t
                ev["is_all_day"] = False
                ev["end_time"] = _norm_time(parts[1]) if len(parts) > 1 else None
            else:
                await update.message.reply_text(
                    f"{R}פורמט: <code>10:00</code> או <code>14:00-16:00</code>",
                    parse_mode=ParseMode.HTML,
                )
                ctx.user_data["edit_field"] = field
                ok = False
        elif field == "date":
            m = re.match(r"(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?", text)
            if m:
                day, mon = int(m.group(1)), int(m.group(2))
                yr = int(m.group(3)) if m.group(3) else datetime.now(ZoneInfo(TZ)).year
                if yr < 100:
                    yr += 2000
                ev["date"] = f"{yr}-{mon:02d}-{day:02d}"
            else:
                d = await llm_date(mdl, text)
                if d:
                    ev["date"] = d
                else:
                    await update.message.reply_text(
                        f"{R}לא הבנתי. נסה: <code>25.3</code> / מחר",
                        parse_mode=ParseMode.HTML,
                    )
                    ctx.user_data["edit_field"] = field
                    ok = False

        if ok:
            ctx.user_data["editing"] = ev
            mid = ctx.user_data.get("edit_msg_id")
            if mid:
                try:
                    await ctx.bot.edit_message_text(
                        chat_id=update.effective_chat.id,
                        message_id=mid,
                        text=card_edit(ev),
                        parse_mode=ParseMode.HTML,
                        reply_markup=kb_edit(ev),
                    )
                except Exception:
                    pass
            await update.message.reply_text(f"{R}✓ עודכן")

    async def cb_save(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        uid = q.from_user.id
        ev = ctx.user_data.pop("editing", None)
        orig = ctx.user_data.pop("edit_orig", None)
        ctx.user_data.pop("edit_msg_id", None)
        ctx.user_data.pop("edit_field", None)
        if not ev:
            await _safe_edit(
                q.message, f"{R}אין שינויים.", parse_mode=ParseMode.HTML
            )
            return
        # Delete original
        if orig:
            gcal_del(svc, orig["cid"], orig["eid"])
            pop_hist(uid)
        # Create new
        cid = get_cal(uid)
        if not cid:
            await _safe_edit(
                q.message, f"{R}שלח /start.", parse_mode=ParseMode.HTML
            )
            return
        ev = normalize(ev)
        err = validate(ev)
        if err:
            await _safe_edit(
                q.message, f"{R}⚠️  {err}", parse_mode=ParseMode.HTML
            )
            return
        try:
            created = gcal_insert(svc, cid, ev)
        except Exception as exc:
            await _safe_edit(
                q.message, f"{R}⚠️  {str(exc)[:80]}", parse_mode=ParseMode.HTML
            )
            return
        push_hist(uid, created["id"], cid, ev)
        await q.edit_message_text(
            card_ok(ev, created.get("htmlLink", "")),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_done(),
        )

    # ── Error ──

    async def on_error(update, ctx: ContextTypes.DEFAULT_TYPE):
        if isinstance(ctx.error, (TimedOut, NetworkError)):
            log.warning("Network: %s", ctx.error)
            return
        log.error(
            "Unhandled error: %s\n%s",
            ctx.error,
            "".join(traceback.format_exception(ctx.error)),
        )

    # ── Post-init ──

    async def post_init(app: Application):
        await app.bot.set_my_commands(CMDS)
        log.info("Bot ready — %d registered users", user_count())

    # ── Build ──

    app = (
        Application.builder()
        .token(os.environ["TELEGRAM_BOT_TOKEN"])
        .post_init(post_init)
        .build()
    )

    app.add_handler(
        ConversationHandler(
            entry_points=[
                CommandHandler("start", cmd_start),
                CommandHandler("setup", cmd_setup),
            ],
            states={
                ST_CAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_cal)]
            },
            fallbacks=[CommandHandler("help", cmd_help)],
        )
    )
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("undo", cmd_undo))
    app.add_handler(CommandHandler("status", cmd_status))

    app.add_handler(CallbackQueryHandler(cb_del, pattern="^a:del$"))
    app.add_handler(CallbackQueryHandler(cb_edit_start, pattern="^a:edit$"))
    app.add_handler(
        CallbackQueryHandler(cb_edit_field, pattern="^e:(title|date|time|loc|desc)$")
    )
    app.add_handler(CallbackQueryHandler(cb_save, pattern="^e:save$"))

    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_doc))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_error_handler(on_error)
    return app


# ═══════════════════════════════════════
#  Main
# ═══════════════════════════════════════

def main():
    for v in ("TELEGRAM_BOT_TOKEN", "GEMINI_API_KEY", "GOOGLE_SA_JSON_B64", "DATABASE_URL"):
        if not os.environ.get(v):
            sys.exit(f"Missing env var: {v}")
    init_db()
    svc, email = init_google()
    mdl = init_gemini()
    log.info("SA: %s | Model: %s", email, GEMINI_MODEL)
    app = build_app(svc, email, mdl)
    log.info("Starting polling…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
