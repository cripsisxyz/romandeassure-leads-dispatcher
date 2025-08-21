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
        "strategy": "weighted_random",
        "receivers": [{"email": DEFAULT_TO, "pourcentage": 100}],
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
            payload_json TEXT NOT NULL
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
                "enabled": bool(bal.get("enabled", True)),
                "strategy": bal.get("strategy", "weighted_random"),
                "receivers": receivers,
            }
        }
    else:
        CONFIG = {
            "balancing": {
                "enabled": True,
                "strategy": "weighted_random",
                "receivers": [{"email": DEFAULT_TO, "pourcentage": 100}],
            }
        }

def pick_recipient() -> str:
    bal = CONFIG["balancing"]
    if not bal.get("enabled", True):
        return DEFAULT_TO
    recs = [r for r in bal["receivers"] if r.get("pourcentage", 0) > 0]
    if not recs:
        return DEFAULT_TO
    weights = [r["pourcentage"] for r in recs]
    return random.choices([r["email"] for r in recs], weights=weights, k=1)[0]

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
        "headers": {"X-RA-Lead": "register_lead", "X-RA-Backup-ID": str(backup_id)},
        "attachment": [{
            "name": f"lead_{backup_id}.json",
            "content": base64.b64encode(piece_jointe).decode("ascii"),
            "type": "application/json"
        }]
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
    to_email = pick_recipient()
    backup_id = sauvegarder_lead_sqlite(lead, to_email)
    try:
        envoyer_email_brevo(lead, backup_id, to_email)
        inc_stat(to_email, True)
        return {"ok": True, "backup_id": backup_id, "email_envoye": True, "to": to_email}
    except Exception as e:
        inc_stat(to_email, False)
        return {"ok": True, "backup_id": backup_id, "email_envoye": False, "to": to_email, "erreur": str(e)}
