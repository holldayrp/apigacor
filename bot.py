"""
GacorMail Bot — REST API
FastAPI wrapper untuk semua fitur bot.
Jalankan: uvicorn api:app --host 0.0.0.0 --port 8000 --reload
"""

import os
import re
import time
import email
import imaplib
import random
import secrets
import socket
import string
import sqlite3
import threading
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, List

import httpx
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from email.header import decode_header

# ============================================================
# KONFIGURASI (samakan dengan bot)
# ============================================================
BOT_TOKEN            = os.getenv("BOT_TOKEN", "8887721278:AAGDbiEssWugcuq2hNApqm0fuTbcCIbY5Io")
API_SECRET_KEY       = os.getenv("API_SECRET_KEY", "gacormail-secret-2025")  # Ganti di production!
ADMIN_IDS            = [7980141797, 1630056409]

IMAP_SERVER          = "imap.gmail.com"
IMAP_PORT            = 993
GMAIL_ADDRESS        = os.getenv("GMAIL_ADDRESS", "imamganteng@bahlil.cfd")
GMAIL_APP_PASSWORD   = os.getenv("GMAIL_APP_PASSWORD", "tmsbdmnfpfdchmyi")

SERVER_NAME          = "Server Bahlil"
CACHE_MAX            = 5000
DEDUP_MAX            = 10000
STARTER_PACK_SLOTS   = 10

QRIS_API_KEY         = os.getenv("QRIS_API_KEY", "6Vws1VAWoTp3rnRNUZAYEVUB06VkhZi9w3bg0RMY")
QRIS_MERCHANT_ID     = os.getenv("QRIS_MERCHANT_ID", "176952001778")
QRIS_BASE_URL        = "https://klikqris.com/api"
PRICE_PER_SLOT       = 100
TOPUP_MIN            = 2000
BONUS_SLOTS_PER_TOPUP = 10
SLOT_EXPIRY_DAYS     = 0
POLL_INTERVAL        = 30
SCAN_BATCH_DELAY     = 0.3

TZ_JAKARTA = ZoneInfo("Asia/Jakarta")
DB_NAME    = "bot_database.db"

# ============================================================
# IN-MEMORY STATE (dipakai bersama dengan bot jika satu proses)
# ============================================================
otp_lock     = threading.Lock()
otp_history  = {}
sent_otp_set = set()
email_owners = {}
user_emails  = {}

# ============================================================
# APP
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- startup ---
    print(f"🚀 GacorMail API started [{now_wib_str()} WIB]")
    print(f"   Docs: http://localhost:8000/docs")
    print(f"   Auth: X-Api-Key header required")
    yield
    # --- shutdown ---
    print(f"🛑 GacorMail API stopped [{now_wib_str()} WIB]")

app = FastAPI(
    title="GacorMail Bot API",
    description="REST API untuk GacorMail Telegram Bot — kelola user, slot, OTP, domain, dan topup.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# AUTH DEPENDENCY
# ============================================================
def verify_api_key(x_api_key: str = Header(..., description="API Key rahasia")):
    if x_api_key != API_SECRET_KEY:
        raise HTTPException(status_code=401, detail="API Key tidak valid")
    return x_api_key

def admin_only(x_api_key: str = Header(...)):
    if x_api_key != API_SECRET_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return x_api_key

# ============================================================
# DB HELPERS
# ============================================================
def now_wib() -> datetime:
    return datetime.now(TZ_JAKARTA)

def now_wib_str() -> str:
    return now_wib().strftime("%Y-%m-%d %H:%M:%S")

def db():
    conn = sqlite3.connect(DB_NAME, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def get_valid_slots(user_id: int) -> int:
    current_wib_str = now_wib_str()
    with db() as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(remaining),0) FROM slot_batches "
            "WHERE user_id=? AND remaining>0 AND (expired_at IS NULL OR expired_at > ?)",
            (user_id, current_wib_str)
        ).fetchone()
    return row[0] if row else 0

def get_user_data(user_id: int) -> dict:
    with db() as conn:
        row = conn.execute(
            "SELECT slots, email_count, otp_count FROM users WHERE user_id=?", (user_id,)
        ).fetchone()
        if not row:
            conn.execute(
                "INSERT OR IGNORE INTO users (user_id, slots, email_count, otp_count) VALUES (?,0,0,0)",
                (user_id,)
            )
            conn.commit()
            batch_cnt = conn.execute(
                "SELECT COUNT(*) FROM slot_batches WHERE user_id=?", (user_id,)
            ).fetchone()[0]
            if batch_cnt == 0:
                _add_slot_batch(user_id, STARTER_PACK_SLOTS, "starterpack")
            return {"slots": get_valid_slots(user_id), "email_count": 0, "otp_count": 0}
    return {"slots": get_valid_slots(user_id), "email_count": row[1], "otp_count": row[2] or 0}

def _expiry_dt() -> Optional[str]:
    if SLOT_EXPIRY_DAYS <= 0:
        return None
    return (now_wib() + timedelta(days=SLOT_EXPIRY_DAYS)).strftime("%Y-%m-%d %H:%M:%S")

def _add_slot_batch(user_id: int, amount: int, source: str):
    exp = _expiry_dt()
    now = now_wib_str()
    with db() as conn:
        conn.execute(
            "INSERT INTO slot_batches (user_id,source,total,remaining,expired_at,created_at) "
            "VALUES (?,?,?,?,?,?)",
            (user_id, source, amount, amount, exp, now)
        )
        conn.execute("UPDATE users SET slots=slots+? WHERE user_id=?", (amount, user_id))
        conn.commit()

def _consume_slot_batch(user_id: int, count: int = 1) -> bool:
    current_wib_str = now_wib_str()
    with db() as conn:
        rows = conn.execute(
            "SELECT id, remaining FROM slot_batches "
            "WHERE user_id=? AND remaining>0 AND (expired_at IS NULL OR expired_at > ?) "
            "ORDER BY created_at ASC",
            (user_id, current_wib_str)
        ).fetchall()
        total_avail = sum(r[1] for r in rows)
        if total_avail < count:
            return False
        to_consume = count
        for batch_id, rem in rows:
            if to_consume <= 0:
                break
            take = min(rem, to_consume)
            conn.execute(
                "UPDATE slot_batches SET remaining=remaining-? WHERE id=?", (take, batch_id)
            )
            to_consume -= take
        conn.execute("UPDATE users SET slots=slots-? WHERE user_id=?", (count, user_id))
        conn.commit()
    return True

def get_domains(active_only: bool = True) -> List[str]:
    with db() as conn:
        if active_only:
            rows = conn.execute(
                "SELECT domain FROM domains WHERE active=1 ORDER BY sort_order ASC, domain ASC"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT domain FROM domains ORDER BY sort_order ASC, domain ASC"
            ).fetchall()
    return [r[0] for r in rows]

def get_domain_label(domain: str) -> str:
    with db() as conn:
        row = conn.execute("SELECT label FROM domain_labels WHERE domain=?", (domain,)).fetchone()
    return row[0] if row else f"@{domain}"

def get_all_user_ids() -> List[int]:
    with db() as conn:
        rows = conn.execute("SELECT user_id FROM users").fetchall()
    return [r[0] for r in rows]

# ============================================================
# OTP HELPERS
# ============================================================
def decode_str(s):
    if not s: return ""
    try:
        decoded = decode_header(s)
        result = ""
        for part, enc in decoded:
            if isinstance(part, bytes):
                result += part.decode(enc or "utf-8", errors="ignore")
            else:
                result += str(part)
        return result
    except:
        return str(s)

def extract_otp(text):
    if not text: return None
    patterns = [
        r'(?i)(?:otp|code|kode|verif|verification|token|pin)[^\d]*(\d{4,8})',
        r'(?i)(\d{4,8})\s+(?:is your|adalah)',
        r'\b(\d{6})\b', r'\b(\d{5})\b', r'\b(\d{4})\b',
    ]
    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            grp = m.groups()
            return grp[0] if grp else m.group()
    return None

def get_email_body(msg):
    body = ""
    try:
        if msg.is_multipart():
            for part in msg.walk():
                ctype = part.get_content_type()
                if ctype == "text/plain":
                    try:
                        body = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                        break
                    except:
                        pass
                elif ctype == "text/html" and not body:
                    try:
                        html = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                        body = re.sub(r"<[^>]+>", " ", html)
                        body = re.sub(r"\s+", " ", body).strip()
                    except:
                        pass
        else:
            try:
                body = msg.get_payload(decode=True).decode("utf-8", errors="ignore")
            except:
                body = str(msg.get_payload())
    except:
        pass
    return body[:2000]

def search_otp_imap(target_email: str) -> Optional[str]:
    date_str = now_wib().strftime("%d-%b-%Y")
    folders  = ['INBOX', '"[Gmail]/All Mail"', '"[Gmail]/Spam"']
    conn     = None
    try:
        conn = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
        conn.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        for folder in folders:
            try:
                status, _ = conn.select(folder, readonly=True)
                if status != "OK":
                    continue
                _, data = conn.search(None, f'(TO "{target_email}" SINCE "{date_str}")')
                if not data or not data[0]:
                    continue
                nums = data[0].split()
                if not nums:
                    continue
                for num in reversed(nums[-10:]):
                    try:
                        _, msg_data = conn.fetch(num, "(RFC822)")
                        if not msg_data or not msg_data[0]:
                            continue
                        raw = msg_data[0]
                        if not isinstance(raw, tuple) or len(raw) < 2:
                            continue
                        msg     = email.message_from_bytes(raw[1])
                        subject = decode_str(msg.get("Subject", ""))
                        body    = get_email_body(msg)
                        otp = extract_otp(body) or extract_otp(subject)
                        if otp:
                            return otp
                    except:
                        continue
            except:
                continue
    except Exception as e:
        print(f"IMAP error [{target_email}]: {e}")
    finally:
        if conn:
            try:
                conn.logout()
            except:
                pass
    return None

def generate_random_email(domain: str) -> str:
    alphanumeric = string.ascii_lowercase + string.digits
    styles = [
        lambda: ''.join(secrets.choice(alphanumeric) for _ in range(random.randint(12, 16))),
        lambda: ''.join(random.choices(string.ascii_lowercase, k=random.randint(6, 8))) + str(secrets.randbelow(900000) + 100000),
        lambda: str(secrets.randbelow(90000) + 10000) + ''.join(random.choices(string.ascii_lowercase, k=random.randint(7, 9))),
        lambda: ''.join(random.choices(string.ascii_lowercase, k=random.randint(11, 15))),
        lambda: ''.join(random.choices(string.ascii_lowercase, k=5)) + str(secrets.randbelow(9000) + 1000) + ''.join(random.choices(string.ascii_lowercase, k=4))
    ]
    username = random.choice(styles)()
    return f"{username}@{domain}"

# ============================================================
# PYDANTIC SCHEMAS
# ============================================================

class GenerateEmailRequest(BaseModel):
    user_id: int = Field(..., description="Telegram user ID")
    domain:  str = Field(..., description="Domain email, contoh: bahlil.cfd")
    count:   int = Field(1, ge=1, le=20, description="Jumlah email (1-20)")

class GetOTPRequest(BaseModel):
    email:   str = Field(..., description="Alamat email target")
    user_id: Optional[int] = Field(None, description="User ID untuk increment stats")

class AddSlotRequest(BaseModel):
    user_id: int = Field(..., description="Target user ID")
    amount:  int = Field(..., ge=1, description="Jumlah slot yang ditambahkan")
    source:  str = Field("admin", description="Sumber slot: admin/topup/starterpack")

class SetSlotsRequest(BaseModel):
    user_id: int
    amount:  int = Field(..., ge=0)

class TopupRequest(BaseModel):
    user_id: int  = Field(..., description="User ID pembeli")
    amount:  int  = Field(..., ge=2000, description="Nominal dalam Rupiah")

class CompleteOrderRequest(BaseModel):
    order_id: str

class AddDomainRequest(BaseModel):
    domain: str = Field(..., description="Nama domain, contoh: surabaya.cfd")
    label:  str = Field("", description="Label tombol, default @domain")

class UpdateDomainRequest(BaseModel):
    old_domain: str
    new_domain: str

class SetDomainLabelRequest(BaseModel):
    domain: str
    label:  str

class BroadcastRequest(BaseModel):
    message: str = Field(..., description="Teks pesan broadcast (Markdown)")

class SetConfigRequest(BaseModel):
    price_per_slot:       Optional[int] = None
    topup_min:            Optional[int] = None
    bonus_slots_per_topup: Optional[int] = None
    slot_expiry_days:     Optional[int] = None

# ============================================================
# ROUTES — HEALTH
# ============================================================

@app.get("/", tags=["Health"])
def root():
    return {"status": "ok", "service": "GacorMail Bot API", "time_wib": now_wib_str()}

@app.get("/health", tags=["Health"])
def health():
    # Cek DB
    try:
        with db() as conn:
            conn.execute("SELECT 1").fetchone()
        db_ok = True
    except:
        db_ok = False
    return {
        "status": "ok" if db_ok else "degraded",
        "db": "connected" if db_ok else "error",
        "time_wib": now_wib_str(),
    }

# ============================================================
# ROUTES — USER
# ============================================================

@app.get("/users/{user_id}", tags=["User"], summary="Get data user")
def get_user(user_id: int, _=Depends(verify_api_key)):
    data = get_user_data(user_id)
    batches = _get_batches(user_id)
    return {
        "user_id":     user_id,
        "slots":       data["slots"],
        "email_count": data["email_count"],
        "otp_count":   data["otp_count"],
        "batches":     batches,
    }

@app.get("/users", tags=["User"], summary="List semua user ID")
def list_users(_=Depends(admin_only)):
    ids = get_all_user_ids()
    return {"total": len(ids), "user_ids": ids}

@app.get("/users/top/otp", tags=["User"], summary="Leaderboard OTP")
def top_otp(limit: int = Query(10, ge=1, le=50), _=Depends(verify_api_key)):
    with db() as conn:
        rows = conn.execute(
            "SELECT user_id, otp_count FROM users ORDER BY otp_count DESC LIMIT ?", (limit,)
        ).fetchall()
    return [{"rank": i+1, "user_id": r[0], "otp_count": r[1]} for i, r in enumerate(rows)]

def _get_batches(user_id: int) -> list:
    current_wib_str = now_wib_str()
    with db() as conn:
        rows = conn.execute(
            "SELECT source, total, remaining, expired_at, created_at FROM slot_batches "
            "WHERE user_id=? AND remaining>0 AND (expired_at IS NULL OR expired_at > ?) "
            "ORDER BY created_at ASC",
            (user_id, current_wib_str)
        ).fetchall()
    return [
        {"source": r[0], "total": r[1], "remaining": r[2],
         "expired_at": r[3] or "permanen", "created_at": r[4]}
        for r in rows
    ]

# ============================================================
# ROUTES — SLOT MANAGEMENT
# ============================================================

@app.post("/slots/add", tags=["Slot"], summary="Tambah slot ke user")
def add_slots(req: AddSlotRequest, _=Depends(admin_only)):
    get_user_data(req.user_id)  # buat user jika belum ada
    _add_slot_batch(req.user_id, req.amount, req.source)
    data = get_user_data(req.user_id)
    return {
        "success":    True,
        "user_id":    req.user_id,
        "added":      req.amount,
        "source":     req.source,
        "total_slots": data["slots"],
    }

@app.post("/slots/set", tags=["Slot"], summary="Set slot user (override)")
def set_slots(req: SetSlotsRequest, _=Depends(admin_only)):
    get_user_data(req.user_id)
    with db() as conn:
        conn.execute("UPDATE users SET slots=? WHERE user_id=?", (req.amount, req.user_id))
        conn.commit()
    return {"success": True, "user_id": req.user_id, "slots": req.amount}

@app.post("/slots/consume", tags=["Slot"], summary="Consume slot user (test/debug)")
def consume_slots(user_id: int, count: int = 1, _=Depends(admin_only)):
    ok = _consume_slot_batch(user_id, count)
    if not ok:
        raise HTTPException(status_code=400, detail="Slot tidak cukup")
    return {"success": True, "user_id": user_id, "consumed": count, "remaining": get_valid_slots(user_id)}

@app.post("/slots/expire", tags=["Slot"], summary="Jalankan expired slot sekarang")
def run_expiry(_=Depends(admin_only)):
    affected = _expire_slots_now()
    return {"success": True, "users_affected": affected, "time_wib": now_wib_str()}

def _expire_slots_now() -> int:
    current_wib_str = now_wib_str()
    affected = 0
    with db() as conn:
        rows = conn.execute(
            "SELECT DISTINCT user_id FROM slot_batches "
            "WHERE remaining>0 AND expired_at IS NOT NULL AND expired_at <= ?",
            (current_wib_str,)
        ).fetchall()
        for (uid,) in rows:
            conn.execute(
                "UPDATE slot_batches SET remaining=0 "
                "WHERE user_id=? AND remaining>0 AND expired_at IS NOT NULL AND expired_at <= ?",
                (uid, current_wib_str)
            )
            valid = conn.execute(
                "SELECT COALESCE(SUM(remaining),0) FROM slot_batches "
                "WHERE user_id=? AND remaining>0 AND (expired_at IS NULL OR expired_at > ?)",
                (uid, current_wib_str)
            ).fetchone()[0]
            conn.execute("UPDATE users SET slots=? WHERE user_id=?", (valid, uid))
            affected += 1
        conn.commit()
    return affected

# ============================================================
# ROUTES — EMAIL GENERATION
# ============================================================

@app.post("/email/generate", tags=["Email"], summary="Buat email baru")
def generate_email(req: GenerateEmailRequest, _=Depends(verify_api_key)):
    active_domains = get_domains(active_only=True)
    if req.domain not in active_domains:
        raise HTTPException(status_code=400, detail=f"Domain '{req.domain}' tidak tersedia")

    data = get_user_data(req.user_id)
    if data["slots"] <= 0:
        raise HTTPException(status_code=402, detail="Slot habis. Lakukan topup terlebih dahulu.")

    count    = min(req.count, data["slots"])
    created  = []

    for _ in range(count):
        current_slots = get_valid_slots(req.user_id)
        if current_slots <= 0:
            break
        em = generate_random_email(req.domain)
        ok = _consume_slot_batch(req.user_id, 1)
        if not ok:
            break
        with otp_lock:
            email_owners[em] = req.user_id
        if req.user_id not in user_emails:
            user_emails[req.user_id] = []
        user_emails[req.user_id].append(em)
        # increment email count
        with db() as conn:
            conn.execute("UPDATE users SET email_count=email_count+1 WHERE user_id=?", (req.user_id,))
            conn.commit()
        created.append(em)

    return {
        "success":        True,
        "user_id":        req.user_id,
        "emails_created": created,
        "count":          len(created),
        "slots_remaining": get_valid_slots(req.user_id),
    }

@app.get("/email/list/{user_id}", tags=["Email"], summary="Daftar email aktif user (sesi)")
def list_emails(user_id: int, _=Depends(verify_api_key)):
    emails = user_emails.get(user_id, [])
    return {"user_id": user_id, "emails": emails, "total": len(emails)}

@app.delete("/email/delete/{user_id}", tags=["Email"], summary="Hapus semua email user dari sesi")
def delete_emails(user_id: int, _=Depends(verify_api_key)):
    emails = user_emails.get(user_id, [])
    count  = len(emails)
    with otp_lock:
        for em in emails:
            em_lower = em.lower()
            otp_history.pop(em_lower, None)
            to_remove = {k for k in sent_otp_set if k.startswith(f"{em_lower}:")}
            sent_otp_set.difference_update(to_remove)
            email_owners.pop(em, None)
    user_emails[user_id] = []
    return {"success": True, "user_id": user_id, "deleted_count": count}

# ============================================================
# ROUTES — OTP
# ============================================================

@app.post("/otp/get", tags=["OTP"], summary="Ambil OTP dari inbox email")
async def get_otp(req: GetOTPRequest, _=Depends(verify_api_key)):
    em = req.email.strip().lower()
    if "@" not in em:
        raise HTTPException(status_code=400, detail="Format email tidak valid")

    # Cek cache dulu
    with otp_lock:
        for otp in reversed(otp_history.get(em, [])):
            sent_key = f"{em}:{otp}"
            if sent_key not in sent_otp_set:
                sent_otp_set.add(sent_key)
                if req.user_id:
                    with db() as conn:
                        conn.execute(
                            "UPDATE users SET otp_count=otp_count+1 WHERE user_id=?", (req.user_id,)
                        )
                        conn.commit()
                return {"success": True, "email": em, "otp": otp, "source": "cache"}

    # Search IMAP
    loop = asyncio.get_running_loop()
    otp  = await loop.run_in_executor(None, search_otp_imap, em)

    if not otp:
        raise HTTPException(status_code=404, detail="OTP belum ditemukan. Coba lagi sebentar.")

    with otp_lock:
        if em not in otp_history:
            otp_history[em] = []
        if otp not in otp_history[em]:
            otp_history[em].append(otp)
        sent_otp_set.add(f"{em}:{otp}")

    if req.user_id:
        with db() as conn:
            conn.execute("UPDATE users SET otp_count=otp_count+1 WHERE user_id=?", (req.user_id,))
            conn.execute("UPDATE bot_stats SET value=CAST(value AS INTEGER)+1 WHERE key='total_otp'")
            conn.commit()

    return {"success": True, "email": em, "otp": otp, "source": "imap"}

@app.get("/otp/cache/{email}", tags=["OTP"], summary="Lihat OTP tersimpan di cache")
def get_otp_cache(email: str, _=Depends(verify_api_key)):
    em = email.strip().lower()
    with otp_lock:
        cached = list(otp_history.get(em, []))
    return {"email": em, "cached_otps": cached, "count": len(cached)}

@app.delete("/otp/cache/{email}", tags=["OTP"], summary="Hapus cache OTP email tertentu")
def clear_otp_cache(email: str, _=Depends(admin_only)):
    em = email.strip().lower()
    with otp_lock:
        otp_history.pop(em, None)
        to_remove = {k for k in sent_otp_set if k.startswith(f"{em}:")}
        sent_otp_set.difference_update(to_remove)
    return {"success": True, "email": em}

# ============================================================
# ROUTES — DOMAIN MANAGEMENT
# ============================================================

@app.get("/domains", tags=["Domain"], summary="List semua domain")
def list_domains(active_only: bool = Query(True), _=Depends(verify_api_key)):
    with db() as conn:
        if active_only:
            rows = conn.execute(
                "SELECT domain, label, active, sort_order, created_at FROM domains "
                "WHERE active=1 ORDER BY sort_order ASC, domain ASC"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT domain, label, active, sort_order, created_at FROM domains "
                "ORDER BY sort_order ASC, domain ASC"
            ).fetchall()
    return [
        {"domain": r[0], "label": r[1], "active": bool(r[2]),
         "sort_order": r[3], "created_at": r[4]}
        for r in rows
    ]

@app.post("/domains", tags=["Domain"], summary="Tambah domain baru")
def add_domain(req: AddDomainRequest, _=Depends(admin_only)):
    domain = req.domain.strip().lower()
    if not domain or "." not in domain:
        raise HTTPException(status_code=400, detail="Domain tidak valid")
    label = req.label.strip() if req.label.strip() else f"@{domain}"
    existing = get_domains(active_only=False)
    if domain in existing:
        raise HTTPException(status_code=409, detail=f"Domain '{domain}' sudah ada")
    with db() as conn:
        max_order = conn.execute("SELECT COALESCE(MAX(sort_order),-1) FROM domains").fetchone()[0]
        conn.execute(
            "INSERT OR IGNORE INTO domains (domain, label, active, sort_order, created_at) "
            "VALUES (?, ?, 1, ?, ?)",
            (domain, label, max_order + 1, now_wib_str())
        )
        conn.execute(
            "INSERT OR REPLACE INTO domain_labels (domain, label) VALUES (?, ?)",
            (domain, label)
        )
        conn.commit()
    return {"success": True, "domain": domain, "label": label}

@app.delete("/domains/{domain}", tags=["Domain"], summary="Hapus domain")
def delete_domain(domain: str, _=Depends(admin_only)):
    domain = domain.strip().lower()
    with db() as conn:
        cursor = conn.execute("DELETE FROM domains WHERE domain=?", (domain,))
        deleted = cursor.rowcount
        conn.execute("DELETE FROM domain_labels WHERE domain=?", (domain,))
        conn.commit()
    if deleted == 0:
        raise HTTPException(status_code=404, detail=f"Domain '{domain}' tidak ditemukan")
    return {"success": True, "deleted_domain": domain}

@app.put("/domains/{domain}/toggle", tags=["Domain"], summary="Toggle aktif/nonaktif domain")
def toggle_domain(domain: str, _=Depends(admin_only)):
    domain = domain.strip().lower()
    with db() as conn:
        row = conn.execute("SELECT active FROM domains WHERE domain=?", (domain,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Domain '{domain}' tidak ditemukan")
        new_state = 0 if row[0] else 1
        conn.execute("UPDATE domains SET active=? WHERE domain=?", (new_state, domain))
        conn.commit()
    return {"success": True, "domain": domain, "active": bool(new_state)}

@app.put("/domains/{domain}/label", tags=["Domain"], summary="Update label domain")
def update_domain_label(domain: str, req: SetDomainLabelRequest, _=Depends(admin_only)):
    domain = domain.strip().lower()
    with db() as conn:
        row = conn.execute("SELECT domain FROM domains WHERE domain=?", (domain,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Domain '{domain}' tidak ditemukan")
        conn.execute("UPDATE domains SET label=? WHERE domain=?", (req.label, domain))
        conn.execute(
            "INSERT OR REPLACE INTO domain_labels (domain, label) VALUES (?, ?)",
            (domain, req.label)
        )
        conn.commit()
    return {"success": True, "domain": domain, "label": req.label}

@app.put("/domains/rename", tags=["Domain"], summary="Ganti nama domain")
def rename_domain(req: UpdateDomainRequest, _=Depends(admin_only)):
    old = req.old_domain.strip().lower()
    new = req.new_domain.strip().lower()
    all_domains = get_domains(active_only=False)
    if old not in all_domains:
        raise HTTPException(status_code=404, detail=f"Domain '{old}' tidak ditemukan")
    if new in all_domains:
        raise HTTPException(status_code=409, detail=f"Domain '{new}' sudah digunakan")
    with db() as conn:
        conn.execute("UPDATE domains SET domain=?, label=? WHERE domain=?", (new, f"@{new}", old))
        conn.execute("UPDATE domain_labels SET domain=?, label=? WHERE domain=?", (new, f"@{new}", old))
        conn.commit()
    return {"success": True, "old_domain": old, "new_domain": new}

# ============================================================
# ROUTES — TOPUP / PAYMENT
# ============================================================

@app.post("/topup/create", tags=["Topup"], summary="Buat order topup QRIS")
async def create_topup(req: TopupRequest, _=Depends(verify_api_key)):
    global BONUS_SLOTS_PER_TOPUP, PRICE_PER_SLOT
    if req.amount < TOPUP_MIN:
        raise HTTPException(status_code=400, detail=f"Minimal topup Rp{TOPUP_MIN:,}")

    get_user_data(req.user_id)

    slots     = req.amount // PRICE_PER_SLOT
    bonus     = BONUS_SLOTS_PER_TOPUP
    total_get = slots + bonus
    ts        = now_wib().strftime("%Y%m%d%H%M%S")
    order_id  = f"SLOT-{req.user_id}-{ts}"

    loop = asyncio.get_running_loop()
    resp = await loop.run_in_executor(
        None,
        lambda: _qris_create(order_id, req.amount, f"Topup {total_get} slot @{req.user_id}")
    )

    if not resp or not resp.get("status"):
        err = resp.get("message", "Unknown") if resp else "Timeout"
        raise HTTPException(status_code=502, detail=f"QRIS error: {err}")

    data         = resp["data"]
    total_amount = int(float(data["total_amount"]))
    qris_url     = data.get("qris_url", "")
    expired_at   = data.get("expired_at", "-")
    signature    = data.get("signature", "")

    with db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO topup_orders "
            "(order_id,user_id,amount,slots,bonus,status,signature,created_at) VALUES (?,?,?,?,?,'PENDING',?,?)",
            (order_id, req.user_id, total_amount, slots, bonus, signature, now_wib_str())
        )
        conn.commit()

    return {
        "success":      True,
        "order_id":     order_id,
        "user_id":      req.user_id,
        "amount":       total_amount,
        "slots":        slots,
        "bonus":        bonus,
        "total_slots":  total_get,
        "qris_url":     qris_url,
        "expired_at":   expired_at,
    }

@app.get("/topup/status/{order_id}", tags=["Topup"], summary="Cek status pembayaran")
async def check_topup_status(order_id: str, _=Depends(verify_api_key)):
    loop = asyncio.get_running_loop()
    resp = await loop.run_in_executor(None, lambda: _qris_status(order_id))
    if not resp or not resp.get("status"):
        raise HTTPException(status_code=502, detail="Gagal cek status QRIS")
    return {"order_id": order_id, "qris_response": resp}

@app.post("/topup/complete", tags=["Topup"], summary="Manual complete order (admin)")
def complete_topup(req: CompleteOrderRequest, _=Depends(admin_only)):
    with db() as conn:
        cursor = conn.execute(
            "UPDATE topup_orders SET status='SUCCESS', paid_at=? WHERE order_id=? AND status='PENDING'",
            (now_wib_str(), req.order_id)
        )
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Order tidak ditemukan atau sudah diproses")
        row = conn.execute(
            "SELECT user_id, slots, bonus FROM topup_orders WHERE order_id=?", (req.order_id,)
        ).fetchone()
        conn.commit()

    if not row:
        raise HTTPException(status_code=404, detail="Order tidak ditemukan")

    user_id, slots, bonus = row
    total = slots + (bonus or 0)
    _add_slot_batch(user_id, total, "topup")
    data = get_user_data(user_id)

    return {
        "success":      True,
        "order_id":     req.order_id,
        "user_id":      user_id,
        "slots_added":  total,
        "total_slots":  data["slots"],
    }

@app.get("/topup/orders", tags=["Topup"], summary="List semua order topup")
def list_orders(
    status: Optional[str] = Query(None, description="Filter: PENDING/SUCCESS/EXPIRED"),
    limit:  int = Query(50, ge=1, le=500),
    _=Depends(admin_only)
):
    with db() as conn:
        if status:
            rows = conn.execute(
                "SELECT order_id, user_id, amount, slots, bonus, status, created_at, paid_at "
                "FROM topup_orders WHERE status=? ORDER BY created_at DESC LIMIT ?",
                (status.upper(), limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT order_id, user_id, amount, slots, bonus, status, created_at, paid_at "
                "FROM topup_orders ORDER BY created_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
    return [
        {"order_id": r[0], "user_id": r[1], "amount": r[2], "slots": r[3],
         "bonus": r[4], "status": r[5], "created_at": r[6], "paid_at": r[7]}
        for r in rows
    ]

def _qris_create(order_id, amount, keterangan="Topup Slot"):
    headers = {
        "Content-Type": "application/json",
        "x-api-key": QRIS_API_KEY,
        "id_merchant": QRIS_MERCHANT_ID,
    }
    try:
        r = httpx.post(
            f"{QRIS_BASE_URL}/qris/create",
            json={"order_id": order_id, "id_merchant": QRIS_MERCHANT_ID,
                  "amount": amount, "keterangan": keterangan},
            headers=headers, timeout=15
        )
        return r.json()
    except Exception as e:
        print(f"QRIS create error: {e}")
        return None

def _qris_status(order_id):
    try:
        r = httpx.get(
            f"{QRIS_BASE_URL}/qris/status/{order_id}",
            headers={"x-api-key": QRIS_API_KEY, "id_merchant": QRIS_MERCHANT_ID},
            timeout=10
        )
        return r.json()
    except Exception as e:
        print(f"QRIS status error: {e}")
        return None

# ============================================================
# ROUTES — STATS & CONFIG
# ============================================================

@app.get("/stats", tags=["Admin"], summary="Statistik bot")
def get_stats(_=Depends(admin_only)):
    total_users   = len(get_all_user_ids())
    active_emails = len(email_owners)
    with db() as conn:
        total_slots = conn.execute("SELECT SUM(slots) FROM users").fetchone()[0] or 0
        total_otp   = conn.execute(
            "SELECT CAST(value AS INTEGER) FROM bot_stats WHERE key='total_otp'"
        ).fetchone()[0] or 0
        topup_row   = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(amount),0) FROM topup_orders WHERE status='SUCCESS'"
        ).fetchone()
    return {
        "total_users":   total_users,
        "active_emails": active_emails,
        "total_slots":   total_slots,
        "total_otp":     total_otp,
        "topup_success": {
            "count":  topup_row[0],
            "revenue": int(topup_row[1]),
        },
        "config": {
            "price_per_slot":        PRICE_PER_SLOT,
            "topup_min":             TOPUP_MIN,
            "bonus_slots_per_topup": BONUS_SLOTS_PER_TOPUP,
            "slot_expiry_days":      SLOT_EXPIRY_DAYS,
            "poll_interval":         POLL_INTERVAL,
        },
        "time_wib": now_wib_str(),
    }

@app.put("/config", tags=["Admin"], summary="Update konfigurasi bot")
def update_config(req: SetConfigRequest, _=Depends(admin_only)):
    global PRICE_PER_SLOT, TOPUP_MIN, BONUS_SLOTS_PER_TOPUP, SLOT_EXPIRY_DAYS
    updated = {}
    with db() as conn:
        if req.price_per_slot is not None and req.price_per_slot > 0:
            PRICE_PER_SLOT = req.price_per_slot
            updated["price_per_slot"] = PRICE_PER_SLOT
        if req.topup_min is not None and req.topup_min >= 0:
            TOPUP_MIN = req.topup_min
            updated["topup_min"] = TOPUP_MIN
        if req.bonus_slots_per_topup is not None and req.bonus_slots_per_topup >= 0:
            BONUS_SLOTS_PER_TOPUP = req.bonus_slots_per_topup
            conn.execute(
                "UPDATE bot_stats SET value=? WHERE key='bonus_slots_per_topup'",
                (str(BONUS_SLOTS_PER_TOPUP),)
            )
            updated["bonus_slots_per_topup"] = BONUS_SLOTS_PER_TOPUP
        if req.slot_expiry_days is not None and req.slot_expiry_days >= 0:
            SLOT_EXPIRY_DAYS = req.slot_expiry_days
            conn.execute(
                "UPDATE bot_stats SET value=? WHERE key='slot_expiry_days'",
                (str(SLOT_EXPIRY_DAYS),)
            )
            updated["slot_expiry_days"] = SLOT_EXPIRY_DAYS
        conn.commit()
    return {"success": True, "updated": updated}

# ============================================================
# ROUTES — FB CHECKPOINT
# ============================================================

@app.post("/fb/check", tags=["FB Checkpoint"], summary="Cek satu email ke Facebook")
async def fb_check(email_addr: str = Query(..., alias="email"), _=Depends(verify_api_key)):
    result = await _check_fb_checkpoint(email_addr.strip().lower())
    with db() as conn:
        conn.execute(
            "INSERT INTO fb_checkpoint_log (email,status,checked_at) VALUES (?,?,?)",
            (email_addr.strip().lower(), result, now_wib_str())
        )
        conn.commit()
    labels = {
        "ok":         "Email aman, tidak terdeteksi checkpoint",
        "checkpoint": "Email kena checkpoint/suspicious",
        "used":       "Email sudah terdaftar di Facebook",
        "error":      "Gagal cek (error koneksi)",
    }
    return {"email": email_addr, "status": result, "description": labels.get(result, "")}

@app.get("/fb/stats", tags=["FB Checkpoint"], summary="Statistik FB checkpoint")
def fb_stats(_=Depends(admin_only)):
    with db() as conn:
        total  = conn.execute("SELECT COUNT(*) FROM fb_checkpoint_log").fetchone()[0]
        ok_cnt = conn.execute("SELECT COUNT(*) FROM fb_checkpoint_log WHERE status='ok'").fetchone()[0]
        cp_cnt = conn.execute("SELECT COUNT(*) FROM fb_checkpoint_log WHERE status='checkpoint'").fetchone()[0]
        us_cnt = conn.execute("SELECT COUNT(*) FROM fb_checkpoint_log WHERE status='used'").fetchone()[0]
        er_cnt = conn.execute("SELECT COUNT(*) FROM fb_checkpoint_log WHERE status='error'").fetchone()[0]
    return {
        "total": total, "ok": ok_cnt,
        "checkpoint": cp_cnt, "used": us_cnt, "error": er_cnt
    }

async def _check_fb_checkpoint(em: str) -> str:
    url = "https://www.facebook.com/ajax/register/validate_email.php"
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 Chrome/112.0.0.0 Mobile Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.post(
                url, data={"email": em, "validate_only": "1", "__a": "1"}, headers=headers
            )
            text = resp.text.lower()
            if "checkpoint" in text or "suspicious" in text:
                return "checkpoint"
            if "already" in text or "registered" in text or "taken" in text:
                return "used"
            if resp.status_code == 200:
                return "ok"
            return "error"
    except:
        return "error"
