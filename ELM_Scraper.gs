// =============================================================================
// ELM_Scraper.gs
// Intégration de l'API scraper Pages Jaunes dans Apps Script
// =============================================================================
// SETUP : une seule chose à faire → remplacer l'URL ci-dessous
// par l'URL de ton service Render après déploiement.
// =============================================================================

const SCRAPER_API_URL = "https://elm-scraper-api.onrender.com"; // ← ton URL Render

// Timeout polling : le scraping de 58 communes prend ~90min
const POLLING_TIMEOUT_MS  = 120 * 60 * 1000; // 2h max
const POLLING_INTERVAL_MS = 20 * 1000;        // vérifie toutes les 20s

// =============================================================================
// FONCTION PRINCIPALE — à appeler depuis ton code ELM existant
// =============================================================================

/**
 * Lance le scraping et attend le résultat.
 * Bloque jusqu'à la fin du scraping (polling automatique).
 *
 * @param {string[]} communes - Liste des noms de communes
 * @param {number}   dpt      - Numéro de département (ex: 50)
 * @returns {Object} { meta, concurrents } — données prêtes à injecter dans l'ELM
 *
 * Exemple :
 *   const data = scraperConcurrents(["Saint-Lo","Agneaux","Canisy"], 50);
 *   const html = genererSectionConcurrents(data);
 */
function scraperConcurrents(communes, dpt) {
  Logger.log(`[ELM Scraper] Lancement — ${communes.length} communes, dpt ${dpt}`);

  // 1. Vérifie si un résultat récent existe déjà (moins de 30 jours)
  const cache = _getCacheResultat(communes, dpt);
  if (cache) {
    Logger.log(`[ELM Scraper] Résultat en cache utilisé (${cache.meta.date_extraction.slice(0,10)})`);
    return cache;
  }

  // 2. Vérifie que l'API est accessible
  if (!_ping()) {
    throw new Error(
      "API ELM non accessible.\n" +
      "Vérifiez que le service Render est démarré : " + SCRAPER_API_URL + "/ping\n" +
      "(Le démarrage à froid peut prendre 30-60 secondes sur le plan gratuit)"
    );
  }

  // 3. Lance le scraping
  const resp = _post("/scrape", { communes: communes, dpt: dpt });
  if (!resp || resp.error) {
    // Peut-être déjà en cours depuis un appel précédent
    if (resp && resp.error && resp.error.includes("déjà en cours")) {
      Logger.log("[ELM Scraper] Scraping déjà en cours, on attend la fin...");
    } else {
      throw new Error("Erreur lancement : " + JSON.stringify(resp));
    }
  } else {
    Logger.log("[ELM Scraper] Scraping lancé : " + resp.message);
  }

  // 4. Polling jusqu'à la fin
  const deadline = Date.now() + POLLING_TIMEOUT_MS;
  let   lastLogCount = 0;

  while (Date.now() < deadline) {
    Utilities.sleep(POLLING_INTERVAL_MS);

    const status = _get("/status");
    if (!status) continue;

    // Affiche les nouveaux logs serveur
    const logs = status.log || [];
    for (let i = lastLogCount; i < logs.length; i++) {
      Logger.log("  " + logs[i]);
    }
    lastLogCount = logs.length;

    // Erreur côté serveur
    if (status.error) {
      throw new Error("Erreur scraping serveur : " + status.error);
    }

    // Terminé !
    if (!status.running && status.step === "done") {
      Logger.log("[ELM Scraper] Terminé !");
      const result = _get("/result");
      if (!result || result.error) {
        throw new Error("Résultat non disponible après scraping terminé");
      }
      // Met en cache dans PropertiesService
      _setCacheResultat(communes, dpt, result);
      return result;
    }

    // Affiche la progression
    if (status.commune) {
      Logger.log(`  [${status.done}/${status.total}] ${status.commune} — ${status.step}`);
    }
  }

  throw new Error(`Timeout : scraping non terminé après ${POLLING_TIMEOUT_MS/60000} minutes`);
}

// =============================================================================
// GÉNÉRATION HTML POUR L'ELM
// =============================================================================

/**
 * Génère le bloc HTML complet (carte + tableau) à injecter dans l'ELM.
 * Remplace les placeholders dans ton template.
 *
 * Placeholders supportés dans ton template HTML :
 *   {{CARTE_CONCURRENTS}}       → carte Leaflet + tableau
 *   {{NB_CONCURRENTS_TOTAL}}    → nombre total
 *   {{NB_PERSONNES_DEP}}        → nb personnes dépendantes
 *   {{NB_MENAGE}}               → nb ménage/repassage
 *   {{NB_GARDE_ENFANTS}}        → nb garde d'enfants
 *   {{NB_JARDINAGE}}            → nb jardinage/bricolage
 *   {{DATE_EXTRACTION}}         → date du scraping
 *
 * @param {Object} scrapData - Résultat de scraperConcurrents()
 * @param {string} template  - Ton template HTML (optionnel)
 * @returns {string} HTML complet
 */
function genererSectionConcurrents(scrapData, template) {
  if (!scrapData || !scrapData.concurrents) {
    return template
      ? template.replace("{{CARTE_CONCURRENTS}}", "<p>Données non disponibles.</p>")
      : "<p>Données non disponibles.</p>";
  }

  const meta        = scrapData.meta || {};
  const concurrents = scrapData.concurrents || [];
  const stats       = meta.stats_par_categorie || {};

  // Génère carte + tableau
  const carteHtml   = _genererCarte(concurrents, meta);
  const tableauHtml = _genererTableau(concurrents);

  const bloc = carteHtml + tableauHtml;

  if (!template) return bloc;

  return template
    .replace("{{CARTE_CONCURRENTS}}",    bloc)
    .replace("{{NB_CONCURRENTS_TOTAL}}", String(meta.total || concurrents.length))
    .replace("{{NB_PERSONNES_DEP}}",     String(stats["Personnes dependantes"] || 0))
    .replace("{{NB_MENAGE}}",            String(stats["Menage / Repassage"] || 0))
    .replace("{{NB_GARDE_ENFANTS}}",     String(stats["Garde d'enfants"] || 0))
    .replace("{{NB_JARDINAGE}}",         String(stats["Jardinage / Bricolage"] || 0))
    .replace("{{DATE_EXTRACTION}}",      (meta.date_extraction || "").slice(0, 10));
}

// =============================================================================
// GÉNÉRATEURS HTML INTERNES
// =============================================================================

function _genererCarte(concurrents, meta) {
  const nbTotal  = concurrents.length;
  const nbGeo    = concurrents.filter(c => c.coords).length;
  const dateStr  = (meta.date_extraction || "").slice(0, 10);
  const dataJson = JSON.stringify(concurrents).replace(/"/g, "&quot;");

  // La carte est dans un <iframe srcdoc> pour l'isolation CSS/JS
  return `
<div style="margin:24px 0;font-family:Arial,sans-serif;">
  <h3 style="color:#1a3a5c;font-size:14pt;margin-bottom:4px;">
    Répartition géographique de la concurrence
  </h3>
  <p style="color:#666;font-size:9pt;margin-bottom:8px;">
    ${nbTotal} concurrents recensés (${nbGeo} géolocalisés) — 
    Source : Pages Jaunes — ${dateStr}
  </p>
  <iframe
    srcdoc="${_genererHtmlCarte(concurrents)}"
    style="width:100%;height:480px;border:1px solid #dde3ec;border-radius:6px;"
    frameborder="0"
  ></iframe>
</div>`;
}

function _genererHtmlCarte(concurrents) {
  const dataJson = JSON.stringify(concurrents);
  const html = `<!DOCTYPE html>
<html lang="fr"><head>
<meta charset="UTF-8">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"><\/script>
<style>
  *{margin:0;padding:0;box-sizing:border-box}
  #map{width:100%;height:100vh}
  #leg{position:fixed;bottom:12px;right:8px;background:white;padding:10px 12px;
    border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.15);font:12px Arial,sans-serif;z-index:999}
  .li{display:flex;align-items:center;gap:6px;margin-bottom:3px}
  .dot{width:11px;height:11px;border-radius:50%}
</style>
</head><body>
<div id="map"></div>
<div id="leg">
  <b style="display:block;margin-bottom:5px;font-size:11px">Catégories</b>
  <div class="li"><div class="dot" style="background:#e74c3c"></div><span>Personnes dépendantes</span></div>
  <div class="li"><div class="dot" style="background:#3498db"></div><span>Ménage / Repassage</span></div>
  <div class="li"><div class="dot" style="background:#2ecc71"></div><span>Garde d'enfants</span></div>
  <div class="li"><div class="dot" style="background:#f39c12"></div><span>Jardinage / Bricolage</span></div>
</div>
<script>
const C={"Personnes dependantes":"#e74c3c","Menage / Repassage":"#3498db","Garde d'enfants":"#2ecc71","Jardinage / Bricolage":"#f39c12"};
const D=${dataJson};
const map=L.map("map").setView([49.11,-1.09],11);
(function T(p,i){if(i>=p.length)return;const t=p[i];const o={attribution:t.a,maxZoom:19};if(t.s)o.subdomains=t.s;const l=L.tileLayer(t.u,o).addTo(map);l.on("tileerror",()=>{map.removeLayer(l);T(p,i+1)})})([
  {u:"https://tiles.stadiamaps.com/tiles/alidade_smooth/{z}/{x}/{y}{r}.png",a:"© Stadia © OSM"},
  {u:"https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",a:"© CARTO © OSM",s:"abcd"}
],0);
const b=[];
D.filter(c=>c.coords).forEach(c=>{
  const col=C[c.categorie]||"#9b59b6";
  L.circleMarker([c.coords.lat,c.coords.lng],{radius:7,fillColor:col,color:"white",weight:2,fillOpacity:.85})
   .bindPopup("<b>"+esc(c.nom)+"</b>"+(c.adresse?"<br>📍 "+esc(c.adresse):"")+(c.telephone?"<br>📞 "+esc(c.telephone):"")+"<br><small>"+esc(c.categorie)+"</small>")
   .addTo(map);
  b.push([c.coords.lat,c.coords.lng]);
});
if(b.length)map.fitBounds(b,{padding:[25,25]});
function esc(s){return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")}
<\/script>
</body></html>`;

  // Escape pour srcdoc
  return html.replace(/"/g, "&quot;");
}

function _genererTableau(concurrents) {
  const CAT_ORDER  = ["Personnes dependantes","Menage / Repassage","Garde d'enfants","Jardinage / Bricolage"];
  const CAT_COLORS = {"Personnes dependantes":"#e74c3c","Menage / Repassage":"#3498db","Garde d'enfants":"#2ecc71","Jardinage / Bricolage":"#f39c12"};

  // Groupe par catégorie
  const grouped = {};
  concurrents.forEach(c => {
    const cat = c.categorie || "Autre";
    if (!grouped[cat]) grouped[cat] = [];
    grouped[cat].push(c);
  });

  let rows = "";
  let i = 0;
  CAT_ORDER.forEach(cat => {
    (grouped[cat] || []).forEach(c => {
      const bg    = i++ % 2 === 0 ? "#fff" : "#f8fafc";
      const color = CAT_COLORS[cat] || "#888";
      rows += `
      <tr style="background:${bg}">
        <td style="padding:5px 10px;border-bottom:1px solid #eee">${_esc(c.nom)}</td>
        <td style="padding:5px 10px;border-bottom:1px solid #eee">${_esc(c.adresse)}</td>
        <td style="padding:5px 10px;border-bottom:1px solid #eee">${_esc(c.ville || c.commune_scraped)}</td>
        <td style="padding:5px 10px;border-bottom:1px solid #eee">${_esc(c.telephone)}</td>
        <td style="padding:5px 10px;border-bottom:1px solid #eee">
          <span style="background:${color};color:white;padding:2px 7px;border-radius:9px;font-size:10px;font-weight:bold">${_esc(cat)}</span>
        </td>
      </tr>`;
    });
  });

  return `
<div style="margin-top:12px;font-family:Arial,sans-serif;font-size:11px;overflow-x:auto;">
  <table style="width:100%;border-collapse:collapse;">
    <thead>
      <tr style="background:#1a3a5c;color:white">
        <th style="padding:7px 10px;text-align:left">Raison sociale</th>
        <th style="padding:7px 10px;text-align:left">Adresse</th>
        <th style="padding:7px 10px;text-align:left">Commune</th>
        <th style="padding:7px 10px;text-align:left">Téléphone</th>
        <th style="padding:7px 10px;text-align:left">Catégorie</th>
      </tr>
    </thead>
    <tbody>${rows}</tbody>
  </table>
</div>`;
}

// =============================================================================
// CACHE — évite de relancer le scraping si les données sont récentes
// =============================================================================

function _getCacheResultat(communes, dpt) {
  try {
    const props = PropertiesService.getScriptProperties();
    const key   = `scraper_${dpt}_${communes.slice(0,3).map(c=>c.replace(/\s/g,"")).join("_")}`;
    const raw   = props.getProperty(key);
    if (!raw) return null;
    const cached = JSON.parse(raw);
    // Expire après 30 jours
    const age = Date.now() - new Date(cached.meta.date_extraction).getTime();
    if (age > 30 * 24 * 3600 * 1000) return null;
    return cached;
  } catch (e) {
    return null;
  }
}

function _setCacheResultat(communes, dpt, result) {
  try {
    const props = PropertiesService.getScriptProperties();
    const key   = `scraper_${dpt}_${communes.slice(0,3).map(c=>c.replace(/\s/g,"")).join("_")}`;
    // PropertiesService limite à 9kB par propriété — on stocke juste le résumé
    // Le résultat complet est sur le serveur Render (/result)
    const summary = {
      meta:        result.meta,
      concurrents: result.concurrents, // peut être grand — si >9kB, tronquer
    };
    const json = JSON.stringify(summary);
    if (json.length < 9000) {
      props.setProperty(key, json);
    }
    // Pour les gros résultats : stocker dans Drive à la place
    // _sauvegarderDrive(result, communes, dpt);
  } catch (e) {
    Logger.log("Cache non sauvegardé : " + e.message);
  }
}

// Optionnel : sauvegarde le JSON dans Drive pour archivage
function _sauvegarderDrive(result, communes, dpt) {
  try {
    const nom     = `ELM_concurrents_${communes[0].replace(/\s/g,"_")}_${dpt}_${new Date().toISOString().slice(0,10)}.json`;
    const contenu = JSON.stringify(result, null, 2);
    DriveApp.createFile(nom, contenu, MimeType.PLAIN_TEXT);
    Logger.log("JSON sauvegardé dans Drive : " + nom);
  } catch (e) {
    Logger.log("Erreur sauvegarde Drive : " + e.message);
  }
}

// =============================================================================
// HTTP UTILS
// =============================================================================

function _get(path) {
  try {
    const resp = UrlFetchApp.fetch(SCRAPER_API_URL + path, {
      method: "get",
      muteHttpExceptions: true,
    });
    return JSON.parse(resp.getContentText("utf-8"));
  } catch (e) {
    Logger.log("GET " + path + " erreur: " + e.message);
    return null;
  }
}

function _post(path, payload) {
  try {
    const resp = UrlFetchApp.fetch(SCRAPER_API_URL + path, {
      method:      "post",
      contentType: "application/json",
      payload:     JSON.stringify(payload),
      muteHttpExceptions: true,
    });
    return JSON.parse(resp.getContentText("utf-8"));
  } catch (e) {
    Logger.log("POST " + path + " erreur: " + e.message);
    return null;
  }
}

function _ping() {
  const r = _get("/ping");
  return r && r.ok === true;
}

function _esc(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// =============================================================================
// FONCTIONS DE TEST — à lancer depuis l'éditeur Apps Script
// =============================================================================

/** Teste la connexion au serveur Render */
function testPing() {
  if (_ping()) {
    Logger.log("✅ API accessible : " + SCRAPER_API_URL);
  } else {
    Logger.log("❌ API non accessible. Vérifiez l'URL et que le service Render est actif.");
  }
}

/** Teste un scraping rapide sur 2 communes */
function testScrapingRapide() {
  const data = scraperConcurrents(["Saint-Lo", "Agneaux"], 50);
  Logger.log("Résultat : " + data.meta.total + " concurrents");
  Logger.log(JSON.stringify(data.meta.stats_par_categorie));
}

/** Vide le cache pour forcer un nouveau scraping */
function viderCache() {
  PropertiesService.getScriptProperties().deleteAllProperties();
  Logger.log("Cache vidé.");
}
