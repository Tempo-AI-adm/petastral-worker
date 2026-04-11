"""
PetAstral Worker — Render deployment
Processes pending jobs: geocoding → ephemeris → Gemini → save to Supabase.
"""

import json
import os
import time
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify

import astro_calculator

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models/"

GEMINI_SYSTEM_INSTRUCTION = (
    "You are PetAstral's intelligence engine. Generate a professional, realistic "
    "personality and wellness guide combining Western Astrology with Animal Genetics "
    "(breed for dogs, fur color for cats). Tone: professional, technical, realistic. "
    "Use terms like 'behavioral tendencies' and 'astrological characteristics'. "
    "Avoid absolute predictions. Each chapter minimum 300 words with practical daily "
    "examples. Write fluidly with natural document appearance."
)

# ---------------------------------------------------------------------------
# Supabase helpers (uses SERVICE ROLE key — bypasses RLS)
# ---------------------------------------------------------------------------

def _sb_headers(prefer="return=representation"):
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": prefer,
    }


def _sb_url(path):
    return os.environ["SUPABASE_URL"].rstrip("/") + path


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Job lifecycle
# ---------------------------------------------------------------------------

def claim_job(job_id):
    """Atomically set pending → processing. Returns True if claimed."""
    resp = requests.patch(
        _sb_url(f"/rest/v1/jobs?id=eq.{job_id}&status=eq.pending"),
        headers=_sb_headers(),
        json={"status": "processing"},
        timeout=10,
    )
    resp.raise_for_status()
    return len(resp.json()) > 0


def update_job(job_id, patch):
    resp = requests.patch(
        _sb_url(f"/rest/v1/jobs?id=eq.{job_id}"),
        headers=_sb_headers(),
        json=patch,
        timeout=10,
    )
    resp.raise_for_status()


def fail_job(job_id, message):
    update_job(job_id, {
        "status": "failed",
        "error_message": message[:500],
        "completed_at": _now_iso(),
    })


# ---------------------------------------------------------------------------
# Gemini
# ---------------------------------------------------------------------------

def build_gemini_prompt(data, signs):
    hour_display = "não informado" if data.get("hour_unknown") else f"{data['hour']:02d}"
    minute_display = f"{data['minute']:02d}"

    return f"""DADOS DO PET:
Nome: {data['pet_name']}
Tipo: {data['pet_type']}
Raça/Pelagem: {data['breed']}
Sexo: {data['sex']}
Cor: {data.get('pet_color') or 'não informado'}
Marcações: {data.get('pet_markings') or 'não informado'}
Data de Nascimento: {data['day']:02d}/{data['month']:02d}/{data['year']} às {hour_display}:{minute_display}h
Local: {data['city']}, {data['country']}

DADOS ASTRAIS CALCULADOS:
- Sol em {signs['sun']}
- Lua em {signs['moon']}
- Mercúrio em {signs['mercury']}
- Vênus em {signs['venus']}
- Marte em {signs['mars']}
- Júpiter em {signs['jupiter']}
- Saturno em {signs['saturn']}
- Urano em {signs['uranus']}
- Netuno em {signs['neptune']}
- Plutão em {signs['pluto']}
- Elemento Predominante: {signs['dominant_element']}

TAREFA: GERE O GUIA PETASTRAL COMPLETO

ESTRUTURA DO LAUDO:

**0. VISÃO ASTRAL (Resumo Executivo)**
Forneça uma frase resumo para cada dimensão:
- Personalidade:
- Emoções:
- Energia:
- Relacionamento:
Em seguida, liste todos os posicionamentos planetários.

**CAPÍTULOS PRINCIPAIS** (cada capítulo mínimo 300 palavras, com 2-3 subtópicos):

**1. Sol em {signs['sun']}: Essência, Comportamento e Personalidade**
**2. Lua em {signs['moon']}: Emoções, Necessidades e Vínculo com o Tutor**
**3. Elementos Astrológicos: O Ambiente e a Energia Ideal**
**4. Mercúrio em {signs['mercury']}: Como Seu Pet Se Comunica e Processa Informações**
**5. Vênus em {signs['venus']}: Relacionamentos e Conexões**
**6. Marte em {signs['mars']}: Energia, Atividade e Comportamento**
**7. Júpiter em {signs['jupiter']}: Sorte, Descobertas e Expansão**
**8. Saturno em {signs['saturn']}: Desafios, Limites e Aprendizados de Vida**
**9. Urano, Netuno e Plutão: Transformações, Instintos e Propósito do Seu Pet**
**PILAR DE BEM-ESTAR (FINAL): Dicas Práticas para o Bem-Estar**"""


def _call_gemini_model(prompt, model, api_key):
    """Try one model with 3 attempts (5s/10s/20s backoff). Returns text or raises."""
    url = f"{GEMINI_BASE_URL}{model}:generateContent"
    delays = [5, 10, 20]
    for attempt, delay in enumerate(delays, start=1):
        print(f"[Gemini] model={model} attempt {attempt}/3 -> {url}", flush=True)
        try:
            resp = requests.post(
                url,
                params={"key": api_key},
                headers={
                    "User-Agent": "PetAstral-Worker/1.0",
                    "Accept": "application/json",
                },
                json={
                    "system_instruction": {"parts": [{"text": GEMINI_SYSTEM_INSTRUCTION}]},
                    "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                    "generationConfig": {"temperature": 0.7, "maxOutputTokens": 8192},
                },
                timeout=120,
            )
            if resp.status_code == 503 and attempt < len(delays):
                print(f"[Gemini] 503 on attempt {attempt} - body: {resp.text[:500]}", flush=True)
                raise requests.exceptions.HTTPError(
                    f"503 Service Unavailable (attempt {attempt})", response=resp
                )
            resp.raise_for_status()
            result = resp.json()
            try:
                return result["candidates"][0]["content"]["parts"][0]["text"]
            except (KeyError, IndexError) as exc:
                raise RuntimeError(f"Unexpected Gemini response: {result}") from exc
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as exc:
            print(f"[Gemini] error on attempt {attempt}: {exc}", flush=True)
            if attempt < len(delays):
                time.sleep(delay)
            else:
                raise RuntimeError(
                    f"model={model} failed after {len(delays)} attempts: {exc}"
                ) from exc


def call_gemini(prompt):
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")

    primary_model = "gemini-2.5-flash"
    fallback_model = "gemini-2.5-flash-lite"

    try:
        return _call_gemini_model(prompt, primary_model, api_key)
    except RuntimeError as primary_exc:
        print(f"[Gemini] primary model failed: {primary_exc}. Trying fallback {fallback_model}", flush=True)

    return _call_gemini_model(prompt, fallback_model, api_key)


# ---------------------------------------------------------------------------
# Save to Supabase (owners → pets → reports)
# ---------------------------------------------------------------------------

def save_to_supabase(data, signs, report_text):
    headers = _sb_headers()

    # 1. Upsert owner (on conflict email, just return existing)
    owner_headers = {**headers, "Prefer": "resolution=merge-duplicates,return=representation"}
    owner_resp = requests.post(
        _sb_url("/rest/v1/owners?on_conflict=email"),
        headers=owner_headers,
        json={"name": data["owner_name"], "email": data["owner_email"]},
        timeout=15,
    )
    owner_resp.raise_for_status()
    owner_id = owner_resp.json()[0]["id"]

    # 2. Insert pet (includes owner_email for RLS read access later)
    birth_data = {
        k: data[k] for k in ("city", "country", "year", "month", "day", "hour", "minute")
    }
    birth_data["hour_unknown"] = data.get("hour_unknown", False)

    pet_resp = requests.post(
        _sb_url("/rest/v1/pets"),
        headers=headers,
        json={
            "owner_id":      owner_id,
            "owner_email":   data["owner_email"],
            "name":          data["pet_name"],
            "type":          data["pet_type"],
            "breed":         data["breed"],
            "sex":           data["sex"],
            "pet_color":     data.get("pet_color"),
            "pet_markings":  data.get("pet_markings"),
            "birth_data":    birth_data,
        },
        timeout=15,
    )
    pet_resp.raise_for_status()
    pet_id = pet_resp.json()[0]["id"]

    # 3. Insert report
    report_resp = requests.post(
        _sb_url("/rest/v1/reports"),
        headers=headers,
        json={
            "pet_id":      pet_id,
            "signs":       signs,
            "report_text": report_text,
            "created_at":  _now_iso(),
        },
        timeout=15,
    )
    report_resp.raise_for_status()
    report_id = report_resp.json()[0]["id"]

    return report_id, pet_id


# ---------------------------------------------------------------------------
# pet_data mapper (sessionStorage → internal data dict)
# ---------------------------------------------------------------------------

def _map_pet_data(pet_data, email):
    """Convert sessionStorage pet_data format to the internal data dict.

    Required internal fields consumed by build_gemini_prompt / save_to_supabase:
      pet_name, pet_type, breed, sex, pet_color, pet_markings,
      city, country, year, month, day, hour, minute, hour_unknown,
      owner_name, owner_email
    """
    current_year = datetime.now(timezone.utc).year

    # "ano" is collected in the form's step 2 (month + year).
    # Fall back to the current year if the field is absent.
    year = pet_data.get("ano")
    year = int(year) if year else current_year

    return {
        "pet_name":     pet_data.get("nome") or "",
        "pet_type":     pet_data.get("tipo") or "",
        "breed":        pet_data.get("raca") or "",
        "sex":          pet_data.get("sexo") or "não informado",
        "pet_color":    ", ".join(pet_data["cor"]) if isinstance(pet_data.get("cor"), list) else (pet_data.get("cor") or ""),
        "pet_markings": pet_data.get("pelo"),
        "city":         pet_data.get("cidade") or "",
        "country":      "Brazil",
        "year":         year,
        "month":        int(pet_data.get("mes") or 1),
        "day":          int(pet_data.get("dia") or 1),
        "hour":         12,
        "minute":       0,
        "hour_unknown": True,
        "owner_name":   "",
        "owner_email":  email,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "petastral-worker"})


@app.route("/process", methods=["POST", "OPTIONS"])
def process_job():
    # CORS preflight
    if request.method == "OPTIONS":
        resp = jsonify({})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return resp, 204

    body = request.get_json(silent=True) or {}
    job_id = body.get("job_id")
    if not job_id:
        return jsonify({"error": "Missing job_id"}), 400

    # 1. Claim job (pending → processing)
    try:
        if not claim_job(job_id):
            return jsonify({"error": "Job not found or already processing"}), 409
    except Exception as exc:
        return jsonify({"error": f"Claim failed: {exc}"}), 502

    # 2. Fetch input_data from jobs table
    try:
        resp = requests.get(
            _sb_url(f"/rest/v1/jobs?id=eq.{job_id}&select=input_data"),
            headers=_sb_headers(),
            timeout=10,
        )
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            return jsonify({"error": "Job not found"}), 404
        data = rows[0]["input_data"]
    except Exception as exc:
        fail_job(job_id, str(exc))
        return jsonify({"error": f"Fetch failed: {exc}"}), 502

    # 3. Astro calculation (geocoding → ephemeris → signs)
    try:
        raw_signs = astro_calculator.calculate(
            city=data["city"],
            country=data["country"],
            year=data["year"],
            month=data["month"],
            day=data["day"],
            hour=data.get("hour", 12),
            minute=data.get("minute", 0),
        )
    except Exception as exc:
        fail_job(job_id, f"Astro calc failed: {exc}")
        return jsonify({"error": f"Astro calculation failed: {exc}"}), 422

    signs = {
        "sun":              raw_signs["sun_sign"],
        "moon":             raw_signs["moon_sign"],
        "mercury":          raw_signs["mercury_sign"],
        "venus":            raw_signs["venus_sign"],
        "mars":             raw_signs["mars_sign"],
        "jupiter":          raw_signs["jupiter_sign"],
        "saturn":           raw_signs["saturn_sign"],
        "uranus":           raw_signs["uranus_sign"],
        "neptune":          raw_signs["neptune_sign"],
        "pluto":            raw_signs["pluto_sign"],
        "dominant_element": raw_signs["dominant_element"],
    }

    # 4. Gemini report generation
    try:
        report_text = call_gemini(build_gemini_prompt(data, signs))
    except Exception as exc:
        fail_job(job_id, f"Gemini failed: {exc}")
        return jsonify({"error": f"Gemini error: {exc}"}), 502

    # 5. Save to owners/pets/reports
    try:
        report_id, pet_id = save_to_supabase(data, signs, report_text)
    except Exception as exc:
        fail_job(job_id, f"Save failed: {exc}")
        return jsonify({"error": f"Supabase save error: {exc}"}), 502

    # 6. Mark job complete
    output = {
        "report_id": report_id,
        "pet_id":    pet_id,
        "signs":     signs,
    }
    update_job(job_id, {
        "status":      "completed",
        "output_data": output,
        "completed_at": _now_iso(),
    })

    return jsonify({"job_id": job_id, "status": "completed", **output})


@app.route("/generate", methods=["POST", "OPTIONS"])
def generate():
    # CORS preflight
    if request.method == "OPTIONS":
        resp = jsonify({})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return resp, 204

    body = request.get_json(silent=True) or {}
    payment_id = body.get("payment_id")
    pet_data   = body.get("pet_data") or {}
    email      = body.get("email") or pet_data.get("email")

    if not payment_id:
        return jsonify({"error": "Missing payment_id"}), 400
    if not pet_data:
        return jsonify({"error": "Missing pet_data"}), 400
    if not email:
        return jsonify({"error": "Missing email"}), 400

    # 1. Map sessionStorage → internal data dict
    data = _map_pet_data(pet_data, email)

    # 2. Astro calculation (geocoding → ephemeris → signs)
    try:
        raw_signs = astro_calculator.calculate(
            city=data["city"],
            country=data["country"],
            year=data["year"],
            month=data["month"],
            day=data["day"],
            hour=data["hour"],
            minute=data["minute"],
        )
    except Exception as exc:
        return jsonify({"error": f"Astro calculation failed: {exc}"}), 422

    signs = {
        "sun":              raw_signs["sun_sign"],
        "moon":             raw_signs["moon_sign"],
        "mercury":          raw_signs["mercury_sign"],
        "venus":            raw_signs["venus_sign"],
        "mars":             raw_signs["mars_sign"],
        "jupiter":          raw_signs["jupiter_sign"],
        "saturn":           raw_signs["saturn_sign"],
        "uranus":           raw_signs["uranus_sign"],
        "neptune":          raw_signs["neptune_sign"],
        "pluto":            raw_signs["pluto_sign"],
        "dominant_element": raw_signs["dominant_element"],
    }

    # 3. Gemini report generation
    try:
        report_text = call_gemini(build_gemini_prompt(data, signs))
    except Exception as exc:
        return jsonify({"error": f"Gemini error: {exc}"}), 502

    # 4. Save owners → pets → reports
    try:
        report_id, _pet_id = save_to_supabase(data, signs, report_text)
    except Exception as exc:
        return jsonify({"error": f"Supabase save error: {exc}"}), 502

    # 5. Link report back to the payment row
    try:
        patch_resp = requests.patch(
            _sb_url(f"/rest/v1/payments?id=eq.{payment_id}"),
            headers=_sb_headers(),
            json={"report_id": report_id},
            timeout=10,
        )
        patch_resp.raise_for_status()
    except Exception as exc:
        # Report is already saved — log but do not fail the request
        print(f"[generate] WARNING: payment patch failed for {payment_id}: {exc}", flush=True)

    return jsonify({"ok": True, "report_id": report_id})


# ---------------------------------------------------------------------------
# Local dev
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
