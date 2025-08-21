# main.py
# pip install -r requirements.txt
import os, json, base64, sqlite3, random, yaml, requests
from datetime import datetime, timezone
from fastapi import FastAPI, Header
from pydantic import BaseModel, Field

app = FastAPI(title="RomandeAssure Leads Dispatcher")

# === CONFIG ENV ===
BREVO_API_KEY = os.getenv("BREVO_API_KEY")
DEFAULT_TO = os.getenv("LEADS_TO", "romandeassure.ch@gmail.com")
DB_PATH = os.getenv("LEADS_SQLITE_PATH", "/data/leads_backup.db")
CONFIG_PATH = os.getenv("LEADS_CONFIG_PATH", "/config/config.yaml")
LOGO_URL = "https://romandeassure.ch/cus-assets/images/logo-romandeassure.png"

CONFIG = {
    "balancing": {
        "enabled": True,
        "strategy": "weighted_random",  # weighted_random | historical | window_deficit
        "receivers": [{"email": DEFAULT_TO, "pourcentage": 100}],
        "window": {"mode": "leads", "size": 500},
        "bootstrap_factor": 0.5,
        "explore_prob": 0.15,
    }
}

# === MODELE LEAD ===
class Lead(BaseModel):
    npa: str
    age: str
    franchise: str
    accident: str
    prenom: str
    nom: str
    telephone: str
    whatsapp: bool
    consentement: bool
    data: dict = Field(default_factory=dict)

# === SQLITE ===
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cree_le TEXT NOT NULL,
            npa TEXT, age TEXT, franchise TEXT, accident TEXT,
            prenom TEXT, nom TEXT, telephone TEXT,
            whatsapp INTEGER, consentement INTEGER,
            routed_to TEXT,
            payload_json TEXT NOT NULL,
            email_sent INTEGER NOT NULL DEFAULT 0
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS recipients_stats (
            email TEXT PRIMARY KEY,
            sent_count INTEGER NOT NULL DEFAULT 0,
            fail_count INTEGER NOT NULL DEFAULT 0,
            last_sent_at TEXT
        )
        """)
        con.commit()
    finally:
        con.close()

def add_columns_if_missing():
    con = sqlite3.connect(DB_PATH)
    try:
        cols = [r[1] for r in con.execute("PRAGMA table_info(leads)").fetchall()]
        if "email_sent" not in cols:
            con.execute("ALTER TABLE leads ADD COLUMN email_sent INTEGER NOT NULL DEFAULT 0")
            con.commit()
    finally:
        con.close()

def sauvegarder_lead_sqlite(lead: Lead, routed_to: str) -> int:
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        cur.execute("""
            INSERT INTO leads (
                cree_le, npa, age, franchise, accident,
                prenom, nom, telephone, whatsapp, consentement,
                routed_to, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now(timezone.utc).isoformat(),
            lead.npa, lead.age, lead.franchise, lead.accident,
            lead.prenom, lead.nom, lead.telephone,
            1 if lead.whatsapp else 0,
            1 if lead.consentement else 0,
            routed_to,
            json.dumps(lead.dict(), ensure_ascii=False),
        ))
        con.commit()
        return cur.lastrowid
    finally:
        con.close()

def inc_stat(email: str, success: bool):
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("""
            INSERT INTO recipients_stats (email, sent_count, fail_count, last_sent_at)
            VALUES (?, 0, 0, NULL)
            ON CONFLICT(email) DO NOTHING
        """, (email,))
        if success:
            con.execute(
                "UPDATE recipients_stats SET sent_count = sent_count + 1, last_sent_at=? WHERE email=?",
                (datetime.now(timezone.utc).isoformat(), email),
            )
        else:
            con.execute(
                "UPDATE recipients_stats SET fail_count = fail_count + 1 WHERE email=?",
                (email,),
            )
        con.commit()
    finally:
        con.close()

# === CONFIG LOADER ===
def load_config():
    global CONFIG
    # Valeurs par défaut
    default = {
        "enabled": True,
        "strategy": "weighted_random",
        "receivers": [{"email": DEFAULT_TO, "pourcentage": 100}],
        "window": {"mode": "leads", "size": 500},
        "bootstrap_factor": 0.5,
        "explore_prob": 0.15,
    }
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        bal = (data.get("balancing") or {})
        receivers = [
            {"email": r.get("email"), "pourcentage": int(r.get("pourcentage", 0))}
            for r in (bal.get("receivers") or []) if r.get("email")
        ]
        if not receivers:
            receivers = [{"email": DEFAULT_TO, "pourcentage": 100}]
        total = sum(max(0, r["pourcentage"]) for r in receivers)
        if total <= 0:
            receivers = [{"email": DEFAULT_TO, "pourcentage": 100}]
            total = 100
        # Normalisation à 100 (arrondis)
        receivers = [
            {"email": r["email"], "pourcentage": round(r["pourcentage"] * 100 / total)}
            for r in receivers
        ]
        diff = 100 - sum(r["pourcentage"] for r in receivers)
        if diff != 0:
            receivers[0]["pourcentage"] += diff

        CONFIG = {
            "balancing": {
                "enabled": bool(bal.get("enabled", default["enabled"])),
                "strategy": str(bal.get("strategy", default["strategy"])),
                "receivers": receivers,
                "window": {
                    "mode": (bal.get("window", {}) or {}).get("mode", default["window"]["mode"]),
                    "size": int((bal.get("window", {}) or {}).get("size", default["window"]["size"])),
                },
                "bootstrap_factor": float(bal.get("bootstrap_factor", default["bootstrap_factor"])),
                "explore_prob": float(bal.get("explore_prob", default["explore_prob"])),
            }
        }
    else:
        CONFIG = {"balancing": default}

# === ELECTION DU DESTINATAIRE ===
def pick_recipient_weighted() -> str:
    bal = CONFIG["balancing"]
    if not bal.get("enabled", True):
        return DEFAULT_TO
    recs = [r for r in bal["receivers"] if r.get("pourcentage", 0) > 0]
    if not recs:
        return DEFAULT_TO
    weights = [r["pourcentage"] for r in recs]
    return random.choices([r["email"] for r in recs], weights=weights, k=1)[0]

def _fetch_stats_map():
    emails_cfg = [r["email"] for r in CONFIG["balancing"]["receivers"] if r.get("pourcentage", 0) > 0]
    if not emails_cfg:
        return {}
    qmarks = ",".join("?" for _ in emails_cfg)
    con = sqlite3.connect(DB_PATH)
    try:
        rows = con.execute(
            f"SELECT email, sent_count FROM recipients_stats WHERE email IN ({qmarks})",
            emails_cfg
        ).fetchall()
        m = {r[0]: r[1] for r in rows}
        for e in emails_cfg:
            m.setdefault(e, 0)
        return m
    finally:
        con.close()

def pick_recipient_historical() -> str:
    """
    Mantiene el % global (histórico total): elige el email con mayor déficit frente a su cuota.
    """
    bal = CONFIG["balancing"]
    if not bal.get("enabled", True):
        return DEFAULT_TO
    recs = [r for r in bal["receivers"] if r.get("pourcentage", 0) > 0]
    if not recs:
        return DEFAULT_TO

    stats = _fetch_stats_map()
    total_sent = sum(stats.get(r["email"], 0) for r in recs)

    if total_sent == 0:
        return pick_recipient_weighted()

    best_email, best_deficit = None, None
    for r in recs:
        email = r["email"]
        pct = r["pourcentage"] / 100.0
        actual = stats.get(email, 0)
        expected = total_sent * pct
        deficit = expected - actual
        if (best_deficit is None) or (deficit > best_deficit):
            best_deficit, best_email = deficit, email

    if best_deficit is not None and best_deficit > 0:
        return best_email

    # Todos sobre-servidos → el de menor ratio actual/objetivo
    best_email, best_ratio = None, None
    for r in recs:
        email = r["email"]
        pct = r["pourcentage"] / 100.0
        actual = stats.get(email, 0)
        expected = max(1e-9, total_sent * pct)
        ratio = actual / expected
        if (best_ratio is None) or (ratio < best_ratio):
            best_ratio, best_email = ratio, email

    return best_email or pick_recipient_weighted()

def _window_counts_leads(limit_n: int):
    """Compte {email: count} sur les N DERNIERS leads avec email_sent=1 (fenêtre)."""
    con = sqlite3.connect(DB_PATH)
    try:
        rows = con.execute("""
            SELECT routed_to FROM leads
            WHERE email_sent=1
            ORDER BY id DESC
            LIMIT ?
        """, (limit_n,)).fetchall()
        counts = {}
        for (email,) in rows:
            counts[email] = counts.get(email, 0) + 1
        return counts
    finally:
        con.close()

def pick_recipient_window_deficit() -> str:
    """
    Applique les pourcentages sur une FENÊTRE (ex: derniers 500 leads).
    - bootstrap_factor: si un destinataire n'apparaît pas dans la fenêtre, simule part de sa quota.
    - explore_prob: petite exploration pondérée pour éviter blocages.
    """
    bal = CONFIG["balancing"]
    if not bal.get("enabled", True):
        return DEFAULT_TO
    recs = [r for r in bal["receivers"] if r.get("pourcentage", 0) > 0]
    if not recs:
        return DEFAULT_TO

    # Exploration aléatoire
    if random.random() < float(bal.get("explore_prob", 0.15)):
        return pick_recipient_weighted()

    win = bal.get("window", {}) or {}
    win_mode = (win.get("mode") or "leads").lower()
    size = int(win.get("size", 500))
    if win_mode != "leads":
        # fallback simple
        return pick_recipient_weighted()

    counts = _window_counts_leads(size)
    total = sum(counts.values())
    if total == 0:
        return pick_recipient_weighted()

    bootstrap = float(bal.get("bootstrap_factor", 0.5))
    best_email, best_deficit = None, None

    for r in recs:
        email = r["email"]
        pct = r["pourcentage"] / 100.0
        actual = counts.get(email, None)
        if actual is None:
            actual = total * pct * bootstrap  # arranque virtual
        expected = total * pct
        deficit = expected - actual
        if (best_deficit is None) or (deficit > best_deficit):
            best_deficit, best_email = deficit, email

    # Si tous déficit <= 0 → choisir le pire ratio (le plus bas)
    if best_deficit is not None and best_deficit <= 0:
        best_email, best_ratio = None, None
        for r in recs:
            email = r["email"]
            pct = r["pourcentage"] / 100.0
            actual = counts.get(email, 0)
            expected = max(1e-9, total * pct)
            ratio = actual / expected
            if (best_ratio is None) or (ratio < best_ratio):
                best_ratio, best_email = ratio, email

    return best_email or pick_recipient_weighted()

def pick_recipient() -> str:
    """Router commun selon la stratégie de config."""
    strat = (CONFIG.get("balancing") or {}).get("strategy", "weighted_random").lower()
    if strat == "historical":
        return pick_recipient_historical()
    if strat == "window_deficit":
        return pick_recipient_window_deficit()
    # default
    return pick_recipient_weighted()

# === EMAIL (BREVO) ===
def envoyer_email_brevo(lead: Lead, backup_id: int, to_email: str):
    if not BREVO_API_KEY:
        raise RuntimeError("BREVO_API_KEY non configurée")

    mapping = {
        "Prénom": lead.prenom,
        "Nom": lead.nom,
        "Téléphone": f"{lead.telephone}{' (WhatsApp)' if lead.whatsapp else ''}",
        "Code postal (NPA)": lead.npa,
        "Âge": lead.age,
        "Franchise": lead.franchise,
        "Accident": lead.accident,
        "Consentement": "Oui" if lead.consentement else "Non",
        "Backup ID": backup_id,
        "Routé vers": to_email,
    }
    tablaRows = ""
    for k, v in mapping.items():
        if v is not None:
            tablaRows += (
                f"<tr><td style='padding:8px;border:1px solid #ddd;font-weight:bold;'>{k}</td>"
                f"<td style='padding:8px;border:1px solid #ddd;'>{v}</td></tr>"
            )

    lignes_meta = ""
    if lead.data:
        for k, v in lead.data.items():
            lignes_meta += (
                f"<tr><td style='padding:8px;border:1px solid #ddd;font-weight:bold;'>{k}</td>"
                f"<td style='padding:8px;border:1px solid #ddd;'>{v}</td></tr>"
            )
    tablaMetadatos = (
        f"<h3 style='color:#223273;'>Plus d'information</h3>"
        f"<table style='width:100%;border-collapse:collapse;margin-top:10px;'>{lignes_meta}</table>"
        if lignes_meta else ""
    )

    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:auto;background:#fff;border:1px solid #e0e0e0;border-radius:8px;overflow:hidden;">
      <div style="background:linear-gradient(90deg,#223273 0%,#637EDB 100%);padding:20px;text-align:center;">
        <img src="{LOGO_URL}" alt="RomandeAssure.ch" style="max-width:200px;">
      </div>
      <div style="padding:20px;">
        <h2 style="color:#223273;">Nouveau Lead Reçu</h2>
        <table style="width:100%;border-collapse:collapse;margin-top:10px;">{tablaRows}</table>
        {tablaMetadatos}
      </div>
      <div style="background:#f9f9f9;text-align:center;color:#999;padding:15px;font-size:12px;">
        RomandeAssure.ch - Plateforme de génération de leads
      </div>
    </div>
    """

    piece_jointe = json.dumps(lead.dict(), ensure_ascii=False, indent=2).encode("utf-8")
    payload = {
        "sender": {"name": "Leads RA", "email": "leads@romandeassure.ch"},
        "to": [{"email": to_email, "name": "Destinataire Leads"}],
        "subject": f"Nouveau lead #{backup_id} - {lead.prenom} {lead.nom}",
        "htmlContent": html,
        "replyTo": {"email": "no-reply@romandeassure.ch"},
        "headers": {"X-RA-Lead": "register_lead", "X-RA-Backup-ID": str(backup_id)}
    }

    r = requests.post(
        "https://api.brevo.com/v3/smtp/email",
        headers={"api-key": BREVO_API_KEY, "accept": "application/json", "content-type": "application/json"},
        json=payload,
        timeout=10
    )
    if r.status_code >= 300:
        raise RuntimeError(f"Erreur Brevo {r.status_code}: {r.text}")

# === API ===
@app.on_event("startup")
def _startup():
    init_db()
    add_columns_if_missing()
    load_config()

@app.get("/api/sante")
def sante():
    return {"statut": "ok"}

@app.post("/api/reload_config")
def reload_config():
    load_config()
    return {"ok": True, "config": CONFIG}

@app.get("/api/balancing_stats")
def balancing_stats():
    con = sqlite3.connect(DB_PATH)
    try:
        rows = con.execute(
            "SELECT email, sent_count, fail_count, last_sent_at FROM recipients_stats ORDER BY email"
        ).fetchall()
        stats = [{"email": r[0], "sent_count": r[1], "fail_count": r[2], "last_sent_at": r[3]} for r in rows]
        return {"ok": True, "stats": stats, "config": CONFIG}
    finally:
        con.close()

@app.post("/api/register_lead")
def enregistrer_lead(lead: Lead, origin: str | None = Header(None)):
    # Choix du destinataire selon la stratégie de config
    to_email = pick_recipient()
    backup_id = sauvegarder_lead_sqlite(lead, to_email)
    sent_ok = 0
    try:
        envoyer_email_brevo(lead, backup_id, to_email)
        sent_ok = 1
        inc_stat(to_email, True)
        return {"ok": True, "backup_id": backup_id, "email_envoye": True, "to": to_email}
    except Exception as e:
        inc_stat(to_email, False)
        return {"ok": True, "backup_id": backup_id, "email_envoye": False, "to": to_email, "erreur": str(e)}
    finally:
        # Persiste le résultat d'envoi pour la fenêtre
        con = sqlite3.connect(DB_PATH)
        try:
            con.execute("UPDATE leads SET email_sent=? WHERE id=?", (sent_ok, backup_id))
            con.commit()
        finally:
            con.close()
