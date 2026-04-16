#!/usr/bin/env python3
"""
ELM Scraper API — Deploiement Render.com
Version job-based (compatible Apps Script asynchrone)
"""

import asyncio
import json
import os
import re
import threading
import time
import urllib.parse
import urllib.request
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime

from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)


# =============================================================================
# CONFIG
# =============================================================================

API_KEY = os.environ.get("ELM_API_KEY", "").strip()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.google.fr/",
}

MAX_PAGES = 20
DELAY_PAGE = 1.5
DELAY_RUB = 2.0
DELAY_COMMUNE = 2.5
JOB_RETENTION_SECONDS = 7 * 24 * 3600  # 7 jours


# =============================================================================
# JOB STATE
# =============================================================================

@dataclass
class JobState:
    job_id: str
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    running: bool = False
    result: dict | None = None
    error: str | None = None
    log: list = field(default_factory=list)
    done: int = 0
    total: int = 0
    commune: str = ""
    step: str = "idle"
    params: dict = field(default_factory=dict)


JOBS = {}
JOBS_LOCK = threading.Lock()


def get_job(job_id):
    with JOBS_LOCK:
        return JOBS.get(job_id)


def set_job(job_id, job):
    with JOBS_LOCK:
        JOBS[job_id] = job


def delete_job(job_id):
    with JOBS_LOCK:
        if job_id in JOBS:
            del JOBS[job_id]


def append_log(job: JobState, msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with JOBS_LOCK:
        job.log.append(line)
        if len(job.log) > 300:
            job.log = job.log[-300:]


def purge_old_jobs():
    now = time.time()
    to_delete = []

    with JOBS_LOCK:
        for job_id, job in JOBS.items():
            try:
                created_ts = datetime.fromisoformat(job.created_at).timestamp()
            except Exception:
                created_ts = now
            if (now - created_ts) > JOB_RETENTION_SECONDS and not job.running:
                to_delete.append(job_id)

        for job_id in to_delete:
            del JOBS[job_id]

    if to_delete:
        print(f"[PURGE] {len(to_delete)} jobs supprimes", flush=True)


# =============================================================================
# AUTH
# =============================================================================

def require_api_key():
    if not API_KEY:
        return None

    provided = request.headers.get("X-ELM-API-KEY", "").strip()
    if provided != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401
    return None


# =============================================================================
# CATEGORIES
# =============================================================================

CATEGORIES = {
    "Personnes dependantes": [
        "service-a-la-personne",
        "services-a-la-personne",
        "aide-a-domicile",
        "accompagnement-personnes-agees",
        "aide-aux-personnes-agees",
        "maintien-a-domicile",
        "soins-a-domicile",
        "assistance-administrative-a-domicile",
    ],
    "Menage Repassage": [
        "menage-repassage-a-domicile",
        "menage-repassage",
        "femme-de-menage",
        "nettoyage-domicile",
    ],
    "Garde enfants": [
        "garde-d-enfants",
        "assistante-maternelle",
        "baby-sitting",
        "creche",
        "micro-creche",
        "halte-garderie",
    ],
    "Jardinage Bricolage": [
        "jardinage",
        "jardinage-entretien-exterieur",
        "entretien-espaces-verts",
        "paysagistes",
        "petits-travaux-bricolage",
        "bricolage-jardinage-petits-travaux",
    ],
}

CAT_LABELS = {
    "Personnes dependantes": "Personnes dependantes",
    "Menage Repassage": "Menage / Repassage",
    "Garde enfants": "Garde d'enfants",
    "Jardinage Bricolage": "Jardinage / Bricolage",
}


# =============================================================================
# HELPERS
# =============================================================================

def normalize_slug(text):
    t = {
        "\xe9": "e", "\xe8": "e", "\xea": "e", "\xeb": "e",
        "\xe0": "a", "\xe2": "a", "\xe4": "a",
        "\xf4": "o", "\xf6": "o", "\xee": "i", "\xef": "i",
        "\xfb": "u", "\xf9": "u", "\xfc": "u",
        "\xe7": "c", "\u0153": "oe", "\xe6": "ae",
        " ": "-", "'": "-", "\u2019": "-", "/": "-",
    }
    s = text.lower()
    for src, dst in t.items():
        s = s.replace(src, dst)
    s = re.sub(r"[^a-z0-9\-]", "", s)
    return re.sub(r"-+", "-", s).strip("-")


def pj_slug(commune, dpt):
    return f"{normalize_slug(commune)}-{dpt}"


def build_url(slug, rubrique, page=1):
    base = f"https://www.pagesjaunes.fr/annuaire/{slug}/{rubrique}"
    return base if page == 1 else f"{base}?page={page}"


# =============================================================================
# PLAYWRIGHT EXTRACTION
# =============================================================================

async def accept_cookies(page):
    for sel in ["#onetrust-accept-btn-handler", "button#acceptAll", "button[id*='accept']"]:
        try:
            btn = await page.query_selector(sel)
            if btn and await btn.is_visible():
                await btn.click()
                await asyncio.sleep(0.7)
                return
        except Exception:
            pass


async def detect_selectors(page):
    try:
        html = await page.content()
    except Exception:
        return {
            "card": "article",
            "nom": ["h2 a", "h3 a", "a[href*='/pros/']"],
            "adresse": ["address"],
            "ville": [],
            "telephone": [],
            "url_fiche": ["a[href*='/pros/']"],
        }

    card_sel = "article"
    for hint in ["bi-pro", "listing-result", "result-item", "search-result"]:
        matches = re.findall(r'class="([^"]*' + hint + r'[^"]*)"', html)
        if matches:
            parts = [c for m in matches for c in m.split() if hint in c.lower()]
            if parts:
                card_sel = f"[class*='{Counter(parts).most_common(1)[0][0]}']"
                break

    return {
        "card": card_sel,
        "nom": ["[class*='denomination']", "[class*='Denomination']", "h2 a", "h3 a", "a[href*='/pros/']"],
        "adresse": ["[class*='address']", "[class*='adresse']", "[itemprop='streetAddress']", "address"],
        "ville": ["[itemprop='addressLocality']", "[class*='city']", "[class*='locality']"],
        "telephone": ["[class*='phone']", "[class*='tel']", "a[href^='tel:']"],
        "url_fiche": ["a[href*='/pros/']", "h2 a", "h3 a"],
    }


async def get_text(el, sels):
    for s in sels:
        try:
            child = await el.query_selector(s)
            if child:
                t = (await child.inner_text()).strip()
                if t:
                    return t
        except Exception:
            pass
    return ""


async def get_attr(el, sels, attr="href"):
    for s in sels:
        try:
            child = await el.query_selector(s)
            if child:
                v = await child.get_attribute(attr)
                if v:
                    return v
        except Exception:
            pass
    return ""


async def extract_cards(page, sels):
    results = []

    try:
        cards = await page.query_selector_all(sels["card"])
    except Exception:
        return results

    if not cards:
        for fb in ["article", "[class*='result']", "[class*='listing']"]:
            try:
                cards = await page.query_selector_all(fb)
                if cards:
                    break
            except Exception:
                continue

    if not cards:
        return results

    for card in cards:
        try:
            nom = await get_text(card, sels["nom"])
            if not nom:
                continue

            adresse = await get_text(card, sels["adresse"])
            ville = await get_text(card, sels["ville"])
            tel = await get_text(card, sels["telephone"])

            if not tel:
                h = await get_attr(card, ["a[href^='tel:']"], "href")
                if h:
                    tel = h.replace("tel:", "")

            href = await get_attr(card, sels["url_fiche"], "href")
            url_fiche = (href if href.startswith("http") else f"https://www.pagesjaunes.fr{href}") if href else ""

            results.append({
                "nom": nom,
                "adresse": adresse.strip(),
                "ville": re.sub(r"\s+", " ", ville).strip(),
                "telephone": tel.strip(),
                "url_fiche": url_fiche,
            })
        except Exception:
            continue

    return results


async def scrape_rubrique(browser, slug, commune_nom, cat_key, rubrique):
    ctx = await browser.new_context(
        extra_http_headers=HEADERS,
        viewport={"width": 1280, "height": 900},
        locale="fr-FR",
    )
    page = await ctx.new_page()

    await page.route(
        "**/*.{png,jpg,jpeg,gif,webp,svg,ico,woff,woff2,ttf,eot}",
        lambda r: r.abort()
    )

    results, seen = [], set()

    sels = {
        "card": "article",
        "nom": ["h2 a"],
        "adresse": ["address"],
        "ville": [],
        "telephone": [],
        "url_fiche": ["a[href*='/pros/']"],
    }

    for page_num in range(1, MAX_PAGES + 1):
        url = build_url(slug, rubrique, page_num)

        try:
            await page.goto(url, wait_until="networkidle", timeout=25000)
        except Exception:
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=5000)
            except Exception:
                break

        await asyncio.sleep(1.5)

        if page_num == 1:
            await accept_cookies(page)
            await asyncio.sleep(0.5)
            sels = await detect_selectors(page)

        try:
            body = await page.inner_text("body")
        except Exception:
            break

        if any(p in body.lower() for p in ["aucun r\xe9sultat", "aucun resultat", "0 r\xe9sultat", "aucun professionnel"]):
            break

        cards = await extract_cards(page, sels)

        if not cards and page.url != url:
            sels = await detect_selectors(page)
            cards = await extract_cards(page, sels)

        if not cards:
            break

        for r in cards:
            key = (r["nom"] + r["adresse"]).lower().strip()
            if key and key not in seen:
                r["commune_scraped"] = commune_nom
                results.append(r)
                seen.add(key)

        if len(cards) < 10:
            break

        await asyncio.sleep(DELAY_PAGE)

    await ctx.close()
    return results


# =============================================================================
# GEOCODAGE
# =============================================================================

BBOX_DEPT = {
    50: (48.55, 49.75, -2.10, 0.05),
    14: (48.75, 49.45, -0.95, 0.50),
    61: (48.10, 48.90, -0.10, 0.95),
    76: (49.25, 50.05, -0.05, 1.80),
    27: (48.65, 49.50, 0.80, 1.90),
    35: (47.60, 48.70, -2.20, -1.00),
    29: (47.70, 48.75, -5.15, -3.30),
    44: (46.85, 47.85, -2.60, -0.90),
}
BBOX_FRANCE = (41.0, 51.5, -5.5, 9.6)

ABBREV = {
    r'\br\b': 'rue',
    r'\bav\b': 'avenue',
    r'\bbd\b': 'boulevard',
    r'\bpl\b': 'place',
    r'\bche\b': 'chemin',
    r'\bch\b': 'chemin',
    r'\bimp\b': 'impasse',
    r'\brt\b': 'route',
    r'\brte\b': 'route',
    r'\bzi\b': 'zone industrielle',
    r'\bza\b': 'zone artisanale',
    r'\bdom\b': 'domaine',
    r'\bham\b': 'hameau',
}


def expand(addr):
    s = addr.lower()
    for pat, rep in ABBREV.items():
        s = re.sub(pat, rep, s)
    return s


def clean_ville(raw, fallback):
    v = re.sub(r'\b\d{5}\b', '', raw)
    v = re.sub(r'\b\d{2}\b', '', v)
    v = re.sub(r'\s+', ' ', v).strip().strip('-')
    return v if v else fallback


def in_bbox(lat, lng, bbox):
    return bbox[0] <= lat <= bbox[1] and bbox[2] <= lng <= bbox[3]


def geocode_fr(adresse, ville_raw, commune_scraped, dpt):
    bbox = BBOX_DEPT.get(dpt, BBOX_FRANCE)
    ville = clean_ville(ville_raw, commune_scraped)
    addr_exp = expand(adresse)

    for q in [
        f"{addr_exp} {ville}",
        f"{addr_exp} {commune_scraped}",
        f"{adresse} {ville}",
        f"{addr_exp} {commune_scraped} {dpt}",
    ]:
        q = q.strip()
        if not q:
            continue

        url = f"https://api-adresse.data.gouv.fr/search/?q={urllib.parse.quote(q)}&limit=3"

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "ELM-API/1.0"})
            with urllib.request.urlopen(req, timeout=6) as r:
                data = json.loads(r.read())
                for feat in data.get("features", []):
                    coords = feat["geometry"]["coordinates"]
                    lat, lng = coords[1], coords[0]
                    score = feat["properties"].get("score", 0)
                    if score >= 0.35 and in_bbox(lat, lng, bbox):
                        return {"lat": lat, "lng": lng, "score": round(score, 2)}
        except Exception:
            pass

        time.sleep(0.1)

    return None


def geocode_all(entries, dpt, job):
    ok = 0

    for i, e in enumerate(entries):
        if e.get("coords"):
            ok += 1
            continue

        r = geocode_fr(
            e.get("adresse", ""),
            e.get("ville", ""),
            e.get("commune_scraped", ""),
            dpt
        )

        if r:
            e["coords"] = {"lat": r["lat"], "lng": r["lng"]}
            e["geocode_score"] = r["score"]
            ok += 1

        time.sleep(0.12)

        if (i + 1) % 20 == 0:
            append_log(job, f"  Geocodage: {i+1}/{len(entries)} ({ok} ok)")

    return ok


# =============================================================================
# CORE SCRAPE
# =============================================================================

def run_scrape(job_id, communes, dpt, no_geocode):
    job = get_job(job_id)
    if not job:
        return

    try:
        from playwright.async_api import async_playwright

        with JOBS_LOCK:
            job.running = True
            job.error = None
            job.result = None
            job.log = []
            job.done = 0
            job.total = len(communes)
            job.step = "starting"

        append_log(job, f"Demarrage — {len(communes)} communes, dpt {dpt}")

        all_results = []
        global_seen = set()

        async def scrape():
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-blink-features=AutomationControlled",
                    ],
                )

                for i_c, commune in enumerate(communes):
                    with JOBS_LOCK:
                        job.step = "scraping"
                        job.commune = commune
                        job.done = i_c

                    append_log(job, f"[{i_c+1}/{len(communes)}] {commune}")
                    slug = pj_slug(commune, dpt)

                    for cat_key, rubriques in CATEGORIES.items():
                        cat_label = CAT_LABELS[cat_key]
                        cat_new = 0

                        for rubrique in rubriques:
                            res = await scrape_rubrique(browser, slug, commune, cat_key, rubrique)

                            for r in res:
                                kl = (r["nom"] + r["adresse"]).lower().strip()
                                kg = normalize_slug(r["nom"]) + r.get("telephone", "")

                                if kl and kl not in global_seen and kg not in global_seen:
                                    r["categorie"] = cat_label
                                    r["rubrique_pj"] = rubrique
                                    all_results.append(r)
                                    global_seen.add(kl)
                                    global_seen.add(kg)
                                    cat_new += 1

                            if res:
                                await asyncio.sleep(DELAY_RUB)

                        if cat_new:
                            append_log(job, f"  {cat_label}: +{cat_new}")

                    append_log(job, f"  Partiel: {len(all_results)} concurrents")

                    if i_c < len(communes) - 1:
                        await asyncio.sleep(DELAY_COMMUNE)

                await browser.close()

        asyncio.run(scrape())

        if not no_geocode:
            with JOBS_LOCK:
                job.step = "geocoding"

            append_log(job, f"Geocodage de {len(all_results)} entrees...")
            ok = geocode_all(all_results, dpt, job)
            append_log(job, f"Geocodage: {ok}/{len(all_results)} reussis")

        stats = {}
        for r in all_results:
            c = r.get("categorie", "Autre")
            stats[c] = stats.get(c, 0) + 1

        total_geo = sum(1 for r in all_results if r.get("coords"))

        result = {
            "meta": {
                "job_id": job_id,
                "communes": communes,
                "departement": dpt,
                "date_extraction": datetime.now().isoformat(),
                "total": len(all_results),
                "total_geocodes": total_geo,
                "stats_par_categorie": stats,
            },
            "concurrents": all_results,
        }

        with JOBS_LOCK:
            job.result = result
            job.running = False
            job.step = "done"
            job.done = len(communes)

        append_log(job, f"Termine — {len(all_results)} concurrents, {total_geo} geocodes")

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        append_log(job, f"ERREUR: {e}\n{tb}")

        with JOBS_LOCK:
            job.error = str(e)
            job.running = False
            job.step = "error"


# =============================================================================
# ROUTES
# =============================================================================

@app.route("/ping", methods=["GET"])
def ping():
    return jsonify({"ok": True, "server": "ELM Scraper API v3.0"})


@app.route("/scrape", methods=["POST", "OPTIONS"])
def scrape():
    if request.method == "OPTIONS":
        return "", 204

    auth_error = require_api_key()
    if auth_error:
        return auth_error

    purge_old_jobs()

    payload = request.get_json(silent=True) or {}
    communes = payload.get("communes", [])
    dpt = int(payload.get("dpt", 50))
    no_geocode = bool(payload.get("no_geocode", False))

    if not communes:
        return jsonify({"error": "Champ 'communes' requis"}), 400

    job_id = str(uuid.uuid4())

    job = JobState(
        job_id=job_id,
        params={
            "communes": communes,
            "dpt": dpt,
            "no_geocode": no_geocode,
        }
    )
    set_job(job_id, job)

    t = threading.Thread(
        target=run_scrape,
        args=(job_id, communes, dpt, no_geocode),
        daemon=True
    )
    t.start()

    return jsonify({
        "ok": True,
        "jobId": job_id,
        "message": f"Scraping lance pour {len(communes)} communes",
        "communes": communes,
        "dpt": dpt,
    })


@app.route("/status", methods=["GET"])
def status():
    auth_error = require_api_key()
    if auth_error:
        return auth_error

    job_id = request.args.get("jobId")
    if not job_id:
        return jsonify({"error": "jobId requis"}), 400

    job = get_job(job_id)
    if not job:
        return jsonify({"error": "job introuvable"}), 404

    return jsonify({
        "jobId": job.job_id,
        "running": job.running,
        "step": job.step,
        "commune": job.commune,
        "done": job.done,
        "total": job.total,
        "error": job.error,
        "has_result": job.result is not None,
        "log": job.log[-50:],
        "created_at": job.created_at,
        "params": job.params,
    })


@app.route("/result", methods=["GET"])
def result():
    auth_error = require_api_key()
    if auth_error:
        return auth_error

    job_id = request.args.get("jobId")
    if not job_id:
        return jsonify({"error": "jobId requis"}), 400

    job = get_job(job_id)
    if not job:
        return jsonify({"error": "job introuvable"}), 404

    if job.result:
        return jsonify(job.result)

    return jsonify({"error": "Aucun resultat disponible"}), 404


@app.route("/result/summary", methods=["GET"])
def result_summary():
    auth_error = require_api_key()
    if auth_error:
        return auth_error

    job_id = request.args.get("jobId")
    if not job_id:
        return jsonify({"error": "jobId requis"}), 400

    job = get_job(job_id)
    if not job:
        return jsonify({"error": "job introuvable"}), 404

    if not job.result:
        return jsonify({"error": "Aucun resultat"}), 404

    meta = job.result.get("meta", {})
    return jsonify({
        "jobId": job.job_id,
        "total": meta.get("total", 0),
        "total_geocodes": meta.get("total_geocodes", 0),
        "date_extraction": meta.get("date_extraction", ""),
        "stats_par_categorie": meta.get("stats_par_categorie", {}),
    })


@app.route("/jobs", methods=["GET"])
def jobs():
    auth_error = require_api_key()
    if auth_error:
        return auth_error

    purge_old_jobs()

    with JOBS_LOCK:
        out = []
        for job in JOBS.values():
            out.append({
                "jobId": job.job_id,
                "created_at": job.created_at,
                "running": job.running,
                "step": job.step,
                "done": job.done,
                "total": job.total,
                "commune": job.commune,
                "error": job.error,
                "has_result": job.result is not None,
                "params": job.params,
            })

    out.sort(key=lambda x: x["created_at"], reverse=True)
    return jsonify({"jobs": out})


@app.route("/job/delete", methods=["POST"])
def delete_job_route():
    auth_error = require_api_key()
    if auth_error:
        return auth_error

    payload = request.get_json(silent=True) or {}
    job_id = payload.get("jobId")
    if not job_id:
        return jsonify({"error": "jobId requis"}), 400

    if not get_job(job_id):
        return jsonify({"error": "job introuvable"}), 404

    delete_job(job_id)
    return jsonify({"ok": True, "jobId": job_id})


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
