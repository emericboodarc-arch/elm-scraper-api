#!/usr/bin/env python3
"""
ELM Scraper API — Deploiement Render.com
Version job-based + diagnostics renforces + extraction PJ plus robuste
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

DELAY_COMMUNE = 2.5
JOB_RETENTION_SECONDS = 7 * 24 * 3600  # 7 jours

DEBUG_DUMP_DIR = os.environ.get("ELM_DEBUG_DUMP_DIR", "/tmp/elm_debug")
MAX_DEBUG_HTML_PER_JOB = 12


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
    debug_files: list = field(default_factory=list)


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
# Notes:
# - on met d'abord des slugs PJ observes actuellement
# - on garde quelques variantes/fallbacks ensuite
# =============================================================================

CATEGORIES = {
    "Personnes dependantes": [
        "service-a-la-personne",
        "aide-a-domicile",
        "services-a-domicile-pour-personnes-agees-personnes-dependantes",
        "services-aux-particuliers",
        "maintien-a-domicile",
        "assistance-administrative-a-domicile",
    ],
    "Menage Repassage": [
        "aide-menagere-a-domicile",
        "menage-a-domicile",
        "service-a-la-personne",
        "services-aux-particuliers",
        "femme-de-menage",
        "nettoyage-domicile",
    ],
    "Garde enfants": [
        "garde-d-enfants",
        "baby-sitting",
        "creche",
        "micro-creche",
        "halte-garderie",
        "assistante-maternelle",
    ],
    "Jardinage Bricolage": [
        "petits-travaux-de-bricolage",
        "jardinage",
        "entretien-espaces-verts",
        "paysagistes",
        "bricolage-jardinage-petits-travaux",
        "services-aux-particuliers",
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

def normalize_slug(text: str) -> str:
    s = (text or "").strip().lower()

    repl = {
        "é": "e", "è": "e", "ê": "e", "ë": "e",
        "à": "a", "â": "a", "ä": "a",
        "ô": "o", "ö": "o",
        "î": "i", "ï": "i",
        "ù": "u", "û": "u", "ü": "u",
        "ç": "c", "œ": "oe", "æ": "ae",
        "’": "-", "'": "-", "/": "-", " ": "-",
    }
    for src, dst in repl.items():
        s = s.replace(src, dst)

    s = re.sub(r"[^a-z0-9\-]", "", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s


def normalize_commune_label(text: str) -> str:
    s = re.sub(r"\s+", " ", (text or "").strip())
    return s


def pj_slug(commune, dpt):
    return f"{normalize_slug(commune)}-{dpt}"


def build_annuaire_url(slug, rubrique, page=1):
    base = f"https://www.pagesjaunes.fr/annuaire/{slug}/{rubrique}"
    return base if page == 1 else f"{base}?page={page}"


def build_recherche_url(slug, rubrique, page=1):
    base = f"https://www.pagesjaunes.fr/recherche/{slug}/{rubrique}"
    return base if page == 1 else f"{base}?page={page}"


def dedupe_key(nom, adresse, telephone):
    n = normalize_slug(nom or "")
    a = normalize_slug(adresse or "")
    t = re.sub(r"\D", "", telephone or "")
    return f"{n}|{a}|{t}"


def safe_filename(s: str) -> str:
    s = normalize_slug(s or "")
    return s[:120] if s else "debug"


def ensure_debug_dir():
    os.makedirs(DEBUG_DUMP_DIR, exist_ok=True)
    return DEBUG_DUMP_DIR


def save_debug_text(job: JobState, kind: str, commune: str, rubrique: str, content: str) -> str:
    ensure_debug_dir()

    if len(job.debug_files) >= MAX_DEBUG_HTML_PER_JOB:
        return ""

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"{safe_filename(job.job_id)}__{safe_filename(commune)}__{safe_filename(rubrique)}__{kind}__{ts}.html"
    path = os.path.join(DEBUG_DUMP_DIR, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content or "")

    with JOBS_LOCK:
        job.debug_files.append(path)

    return path


# =============================================================================
# PLAYWRIGHT EXTRACTION
# =============================================================================

async def accept_cookies(page):
    for sel in [
        "#onetrust-accept-btn-handler",
        "button#acceptAll",
        "button[id*='accept']",
        "button[aria-label*='accepter']",
        "button:has-text('Tout accepter')",
        "button:has-text('Accepter')",
    ]:
        try:
            btn = await page.query_selector(sel)
            if btn and await btn.is_visible():
                await btn.click()
                await asyncio.sleep(0.7)
                return
        except Exception:
            pass


async def collect_page_diagnostics(page):
    title = ""
    url = ""
    body_text = ""
    html = ""

    try:
        title = await page.title()
    except Exception:
        pass

    try:
        url = page.url
    except Exception:
        pass

    try:
        body_text = await page.inner_text("body")
    except Exception:
        pass

    try:
        html = await page.content()
    except Exception:
        pass

    pros_links = len(re.findall(r'/pros/\d+', html or ""))

    return {
        "title": title,
        "url": url,
        "body_text": body_text[:8000] if body_text else "",
        "pros_links": pros_links,
        "html": html,
    }


async def extract_entries_via_dom(page):
    """
    Extraction robuste:
    - on part des liens /pros/
    - on remonte vers un conteneur raisonnable
    - on extrait nom / adresse / telephone via JS
    """
    js = r"""
    () => {
      const anchors = Array.from(document.querySelectorAll('a[href*="/pros/"]'));
      const out = [];
      const seen = new Set();

      function clean(s) {
        return (s || '').replace(/\s+/g, ' ').trim();
      }

      function nearestCard(el) {
        let cur = el;
        for (let i = 0; i < 6 && cur; i++, cur = cur.parentElement) {
          if (!cur) break;
          const tag = (cur.tagName || '').toLowerCase();
          const txt = clean(cur.innerText || '');
          if (txt.length > 30 && txt.length < 2500 && ['article','li','section','div'].includes(tag)) {
            return cur;
          }
        }
        return el.parentElement || el;
      }

      function findPhone(text) {
        const m = text.match(/(?:\+33|0)[1-9](?:[\s\.\-]?\d{2}){4}/);
        return m ? clean(m[0]) : '';
      }

      function findAddress(cardText) {
        const lines = cardText.split('\n').map(clean).filter(Boolean);
        for (const line of lines) {
          if (/\b\d{5}\b/.test(line)) {
            return line;
          }
        }
        for (const line of lines) {
          if (/\b(rue|avenue|av\.?|boulevard|bd\.?|place|pl\.?|chemin|route|impasse|all[ée]e|lieu-dit|za|zi)\b/i.test(line)) {
            return line;
          }
        }
        return '';
      }

      function findVille(addressLine, cardText) {
        if (addressLine) {
          const m = addressLine.match(/\b\d{5}\s+(.+)$/);
          if (m) return clean(m[1]);
        }
        const lines = cardText.split('\n').map(clean).filter(Boolean);
        for (const line of lines) {
          const m = line.match(/\b\d{5}\s+(.+)$/);
          if (m) return clean(m[1]);
        }
        return '';
      }

      for (const a of anchors) {
        const href = a.getAttribute('href') || '';
        const pros = href.match(/\/pros\/(\d+)/);
        if (!pros) continue;

        const card = nearestCard(a);
        const cardText = clean(card.innerText || '');
        if (!cardText) continue;

        let nom = clean(a.innerText || '');
        if (!nom || nom.length < 2) {
          const h = card.querySelector('h1,h2,h3,h4,[class*="denomination"],[class*="title"]');
          nom = clean(h ? h.innerText : '');
        }
        if (!nom || nom.length < 2) continue;

        const adresse = findAddress(cardText);
        const ville = findVille(adresse, cardText);
        const telLink = card.querySelector('a[href^="tel:"]');
        const telHref = telLink ? (telLink.getAttribute('href') || '') : '';
        const telephone = telHref ? clean(telHref.replace(/^tel:/, '')) : findPhone(cardText);

        const url_fiche = href.startsWith('http') ? href : ('https://www.pagesjaunes.fr' + href);
        const key = pros[1] + '|' + nom.toLowerCase();

        if (seen.has(key)) continue;
        seen.add(key);

        out.push({
          nom,
          adresse,
          ville,
          telephone,
          url_fiche,
          raw_excerpt: cardText.slice(0, 900),
        });
      }

      return out;
    }
    """
    try:
        return await page.evaluate(js)
    except Exception:
        return []


def body_looks_empty_or_blocked(body: str) -> str:
    txt = (body or "").lower()

    block_patterns = [
        "captcha",
        "vérifiez que vous êtes humain",
        "verifiez que vous etes humain",
        "accès refusé",
        "access denied",
        "forbidden",
        "temporarily unavailable",
    ]
    for p in block_patterns:
        if p in txt:
            return "blocked"

    empty_patterns = [
        "aucun résultat",
        "aucun resultat",
        "0 résultat",
        "0 resultat",
        "aucun professionnel",
    ]
    for p in empty_patterns:
        if p in txt:
            return "empty"

    return ""


async def scrape_single_url(page, url, commune_nom, rubrique, job):
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except Exception:
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=5000)
        except Exception:
            return {
                "entries": [],
                "diag": {
                    "url": url,
                    "title": "",
                    "final_url": getattr(page, "url", url),
                    "pros_links": 0,
                    "reason": "goto_failed",
                },
            }

    await asyncio.sleep(1.0)
    await accept_cookies(page)
    await asyncio.sleep(0.4)

    diag = await collect_page_diagnostics(page)
    reason = body_looks_empty_or_blocked(diag["body_text"])

    entries = await extract_entries_via_dom(page)

    for e in entries:
        e["commune_scraped"] = commune_nom
        e["rubrique_pj"] = rubrique

    return {
        "entries": entries,
        "diag": {
            "url": url,
            "title": diag["title"],
            "final_url": diag["url"],
            "pros_links": diag["pros_links"],
            "reason": reason or "",
            "body_excerpt": diag["body_text"][:1200],
            "html": diag["html"],
        },
    }


async def scrape_rubrique(browser, slug, commune_nom, cat_key, rubrique, max_pages, delay_page, job):
    ctx = await browser.new_context(
        extra_http_headers=HEADERS,
        viewport={"width": 1366, "height": 900},
        locale="fr-FR",
    )
    page = await ctx.new_page()

    await page.route(
        "**/*.{png,jpg,jpeg,gif,webp,svg,ico,woff,woff2,ttf,eot}",
        lambda r: r.abort()
    )

    results = []
    seen = set()
    diag_rows = []

    url_builders = [build_annuaire_url, build_recherche_url]

    for page_num in range(1, max_pages + 1):
        page_entries = []
        page_diag = None

        for builder in url_builders:
            url = builder(slug, rubrique, page_num)
            payload = await scrape_single_url(page, url, commune_nom, rubrique, job)
            entries = payload["entries"]
            page_diag = payload["diag"]

            diag_rows.append({
                "page": page_num,
                "url": page_diag.get("url", ""),
                "final_url": page_diag.get("final_url", ""),
                "title": page_diag.get("title", ""),
                "pros_links": page_diag.get("pros_links", 0),
                "reason": page_diag.get("reason", ""),
                "entries": len(entries),
            })

            if entries:
                page_entries = entries
                break

            if page_diag.get("reason") == "blocked":
                html = page_diag.get("html", "")
                if html:
                    path = save_debug_text(job, "blocked", commune_nom, rubrique, html)
                    if path:
                        append_log(job, f"    DEBUG HTML bloque sauvegarde: {path}")
                break

        if not page_diag:
            break

        append_log(
            job,
            f"    {rubrique} p{page_num} | title='{page_diag.get('title','')[:60]}' | "
            f"pros_links={page_diag.get('pros_links', 0)} | extracted={len(page_entries)}"
        )

        if not page_entries:
            if page_num == 1:
                html = page_diag.get("html", "")
                if html:
                    path = save_debug_text(job, "empty", commune_nom, rubrique, html)
                    if path:
                        append_log(job, f"    DEBUG HTML vide sauvegarde: {path}")
            break

        added = 0
        for r in page_entries:
            key = dedupe_key(r.get("nom"), r.get("adresse"), r.get("telephone"))
            if key and key not in seen:
                results.append(r)
                seen.add(key)
                added += 1

        if added == 0:
            break

        if len(page_entries) < 5:
            break

        await asyncio.sleep(delay_page)

    await ctx.close()
    return results, diag_rows


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
    s = (addr or "").lower()
    for pat, rep in ABBREV.items():
        s = re.sub(pat, rep, s)
    return s


def clean_ville(raw, fallback):
    v = re.sub(r'\b\d{5}\b', '', raw or "")
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
            job.debug_files = []

        append_log(job, f"Demarrage — {len(communes)} communes, dpt {dpt}")

        fast = bool(job.params.get("fast", False))

        if fast:
            max_pages = 2
            rubriques_limit = 2
            delay_page = 0.5
            delay_rub = 0.5
            append_log(job, "Mode FAST active")
        else:
            max_pages = 5
            rubriques_limit = None
            delay_page = 1.2
            delay_rub = 1.2

        all_results = []
        global_seen = set()
        diagnostics = []

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

                    commune = normalize_commune_label(commune)
                    append_log(job, f"[{i_c+1}/{len(communes)}] {commune}")

                    slug = pj_slug(commune, dpt)
                    append_log(job, f"  slug={slug}")

                    for cat_key, rubriques in CATEGORIES.items():
                        current_rubriques = rubriques[:rubriques_limit] if rubriques_limit else list(rubriques)
                        cat_label = CAT_LABELS[cat_key]
                        cat_new = 0

                        for rubrique in current_rubriques:
                            res, diag_rows = await scrape_rubrique(
                                browser=browser,
                                slug=slug,
                                commune_nom=commune,
                                cat_key=cat_key,
                                rubrique=rubrique,
                                max_pages=max_pages,
                                delay_page=delay_page,
                                job=job,
                            )

                            diagnostics.extend([
                                {
                                    "commune": commune,
                                    "categorie": cat_label,
                                    "rubrique": rubrique,
                                    **d,
                                }
                                for d in diag_rows
                            ])

                            brut = len(res)
                            append_log(job, f"    {rubrique}: brut={brut}")

                            for r in res:
                                kl = dedupe_key(r.get("nom"), r.get("adresse"), r.get("telephone"))
                                if kl and kl not in global_seen:
                                    r["categorie"] = cat_label
                                    r["rubrique_pj"] = rubrique
                                    all_results.append(r)
                                    global_seen.add(kl)
                                    cat_new += 1

                            if res:
                                await asyncio.sleep(delay_rub)

                        append_log(job, f"  {cat_label}: +{cat_new}")

                    append_log(job, f"  Partiel: {len(all_results)} concurrents")

                    if i_c < len(communes) - 1:
                        await asyncio.sleep(DELAY_COMMUNE if not fast else 0.5)

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
                "fast": fast,
                "debug_files": list(job.debug_files),
            },
            "concurrents": all_results,
            "diagnostics": diagnostics[-500:],
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
    return jsonify({"ok": True, "server": "ELM Scraper API v3.2-diagnostics"})


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
    fast = bool(payload.get("fast", False))
    no_geocode = bool(payload.get("no_geocode", False)) or fast

    if not communes:
        return jsonify({"error": "Champ 'communes' requis"}), 400

    job_id = str(uuid.uuid4())

    job = JobState(
        job_id=job_id,
        params={
            "communes": communes,
            "dpt": dpt,
            "no_geocode": no_geocode,
            "fast": fast,
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
        "fast": fast,
        "no_geocode": no_geocode,
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
        "debug_files": job.debug_files[-20:],
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
        "fast": meta.get("fast", False),
        "debug_files": meta.get("debug_files", []),
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
                "debug_files": job.debug_files[-10:],
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
