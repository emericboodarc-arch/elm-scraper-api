// =============================================================================
// 14_concurrents.gs — Scraper Pages Jaunes via API Render
// =============================================================================
// URL du service Render (ne pas modifier)
var SCRAPER_API_URL    = "https://elm-scraper-api-1.onrender.com";
var POLLING_TIMEOUT_MS  = 120 * 60 * 1000; // 2h max
var POLLING_INTERVAL_MS = 20 * 1000;        // toutes les 20s

// =============================================================================
// FONCTION PRINCIPALE
// =============================================================================

/**
 * Lance le scraping et attend le resultat.
 * @param {string[]} communes - ex: ["Saint-Lo","Agneaux","Canisy"]
 * @param {number}   dpt      - ex: 50
 * @returns {Object} { meta, concurrents }
 *
 * Exemple d'appel depuis ton orchestrateur :
 *   var data = scraperConcurrents(COMMUNES_ELM, 50);
 *   var html = genererSectionConcurrents(data, monTemplate);
 */
function scraperConcurrents(communes, dpt) {
  Logger.log("[ELM Scraper] Lancement — " + communes.length + " communes, dpt " + dpt);

  // Cache 30 jours — evite de relancer si les donnees sont recentes
  var cache = _getCacheResultat(communes, dpt);
  if (cache) {
    Logger.log("[ELM Scraper] Cache utilise (" + cache.meta.date_extraction.slice(0,10) + ")");
    return cache;
  }

  // Ping — verifie que l'API est accessible (cold start possible ~60s)
  if (!_ping()) {
    throw new Error(
      "API ELM non accessible. Le demarrage a froid peut prendre 60 secondes.\n" +
      "URL : " + SCRAPER_API_URL + "/ping"
    );
  }

  // Lance le scraping
  var resp = _post("/scrape", { communes: communes, dpt: dpt });
  if (!resp || resp.error) {
    if (resp && resp.error && resp.error.indexOf("cours") > -1) {
      Logger.log("[ELM Scraper] Scraping deja en cours, attente...");
    } else {
      throw new Error("Erreur lancement : " + JSON.stringify(resp));
    }
  } else {
    Logger.log("[ELM Scraper] Lance : " + resp.message);
  }

  // Polling jusqu'a la fin
  var deadline     = Date.now() + POLLING_TIMEOUT_MS;
  var lastLogCount = 0;

  while (Date.now() < deadline) {
    Utilities.sleep(POLLING_INTERVAL_MS);
    var status = _get("/status");
    if (!status) continue;

    // Affiche les nouveaux logs du serveur
    var logs = status.log || [];
    for (var i = lastLogCount; i < logs.length; i++) {
      Logger.log("  " + logs[i]);
    }
    lastLogCount = logs.length;

    if (status.error) {
      throw new Error("Erreur scraping serveur : " + status.error);
    }

    if (!status.running && status.step === "done") {
      Logger.log("[ELM Scraper] Termine !");
      var result = _get("/result");
      if (!result || result.error) {
        throw new Error("Resultat non disponible apres fin du scraping");
      }
      _setCacheResultat(communes, dpt, result);
      return result;
    }

    if (status.commune) {
      Logger.log("  [" + status.done + "/" + status.total + "] " + status.commune + " — " + status.step);
    }
  }

  throw new Error("Timeout : scraping non termine apres " + (POLLING_TIMEOUT_MS / 60000) + " minutes");
}

// =============================================================================
// GENERATION HTML POUR L'ELM
// =============================================================================

/**
 * Genere le bloc HTML (carte Leaflet + tableau) a injecter dans le template ELM.
 *
 * Placeholders supportes dans ton template :
 *   {{CARTE_CONCURRENTS}}     -> carte + tableau complets
 *   {{NB_CONCURRENTS_TOTAL}}  -> nombre total
 *   {{NB_PERSONNES_DEP}}      -> nb personnes dependantes
 *   {{NB_MENAGE}}             -> nb menage/repassage
 *   {{NB_GARDE_ENFANTS}}      -> nb garde d'enfants
 *   {{NB_JARDINAGE}}          -> nb jardinage/bricolage
 *   {{DATE_EXTRACTION}}       -> date du scraping (YYYY-MM-DD)
 *
 * @param {Object} scrapData - resultat de scraperConcurrents()
 * @param {string} template  - ton template HTML ELM (optionnel)
 * @returns {string} HTML complet
 */
function genererSectionConcurrents(scrapData, template) {
  if (!scrapData || !scrapData.concurrents) {
    var vide = "<p style='font-family:Arial;color:#999'>Donnees non disponibles.</p>";
    return template ? template.replace("{{CARTE_CONCURRENTS}}", vide) : vide;
  }

  var meta        = scrapData.meta || {};
  var concurrents = scrapData.concurrents || [];
  var stats       = meta.stats_par_categorie || {};
  var bloc        = _genererCarte(concurrents, meta) + _genererTableau(concurrents);

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
// CARTE LEAFLET (iframe srcdoc — fonctionne dans Google Docs/HTML ELM)
// =============================================================================

function _genererCarte(concurrents, meta) {
  var nbTotal = concurrents.length;
  var nbGeo   = concurrents.filter(function(c) { return c.coords; }).length;
  var dateStr = (meta.date_extraction || "").slice(0, 10);

  return '<div style="margin:24px 0;font-family:Arial,sans-serif;">' +
    '<h3 style="color:#1a3a5c;font-size:14pt;margin-bottom:4px;">' +
    'Repartition geographique de la concurrence</h3>' +
    '<p style="color:#666;font-size:9pt;margin-bottom:8px;">' +
    nbTotal + ' concurrents recenses (' + nbGeo + ' geolocalises)' +
    ' — Source : Pages Jaunes — ' + dateStr + '</p>' +
    '<iframe srcdoc="' + _genererHtmlCarte(concurrents) + '" ' +
    'style="width:100%;height:480px;border:1px solid #dde3ec;border-radius:6px;" ' +
    'frameborder="0"></iframe></div>';
}

function _genererHtmlCarte(concurrents) {
  var dataJson = JSON.stringify(concurrents);

  var html = '<!DOCTYPE html><html lang="fr"><head><meta charset="UTF-8">' +
    '<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>' +
    '<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"><\\/script>' +
    '<style>' +
    '*{margin:0;padding:0;box-sizing:border-box}' +
    '#map{width:100%;height:100vh}' +
    '#leg{position:fixed;bottom:12px;right:8px;background:white;padding:10px 12px;' +
    'border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.15);font:12px Arial,sans-serif;z-index:999}' +
    '.li{display:flex;align-items:center;gap:6px;margin-bottom:3px}' +
    '.dot{width:11px;height:11px;border-radius:50%}' +
    '</style></head><body>' +
    '<div id="map"></div>' +
    '<div id="leg">' +
    '<b style="display:block;margin-bottom:5px;font-size:11px">Categories</b>' +
    '<div class="li"><div class="dot" style="background:#e74c3c"></div><span>Personnes dependantes</span></div>' +
    '<div class="li"><div class="dot" style="background:#3498db"></div><span>Menage / Repassage</span></div>' +
    '<div class="li"><div class="dot" style="background:#2ecc71"></div><span>Garde d\'enfants</span></div>' +
    '<div class="li"><div class="dot" style="background:#f39c12"></div><span>Jardinage / Bricolage</span></div>' +
    '</div>' +
    '<script>' +
    'var C={"Personnes dependantes":"#e74c3c","Menage / Repassage":"#3498db","Garde d\'enfants":"#2ecc71","Jardinage / Bricolage":"#f39c12"};' +
    'var D=' + dataJson + ';' +
    'var map=L.map("map").setView([49.11,-1.09],11);' +
    '(function T(p,i){if(i>=p.length)return;' +
    'var t=p[i];var o={attribution:t.a,maxZoom:19};' +
    'if(t.s)o.subdomains=t.s;' +
    'var l=L.tileLayer(t.u,o).addTo(map);' +
    'l.on("tileerror",function(){map.removeLayer(l);T(p,i+1)});})([' +
    '{u:"https://tiles.stadiamaps.com/tiles/alidade_smooth/{z}/{x}/{y}{r}.png",a:"Stadia Maps"},' +
    '{u:"https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",a:"CARTO",s:"abcd"}' +
    '],0);' +
    'var b=[];' +
    'D.filter(function(c){return c.coords}).forEach(function(c){' +
    'var col=C[c.categorie]||"#9b59b6";' +
    'L.circleMarker([c.coords.lat,c.coords.lng],' +
    '{radius:7,fillColor:col,color:"white",weight:2,fillOpacity:.85})' +
    '.bindPopup("<b>"+esc(c.nom)+"</b>"' +
    '+(c.adresse?"<br>"+esc(c.adresse):"")' +
    '+(c.telephone?"<br>"+esc(c.telephone):"")' +
    '+"<br><small>"+esc(c.categorie)+"</small>")' +
    '.addTo(map);' +
    'b.push([c.coords.lat,c.coords.lng]);});' +
    'if(b.length)map.fitBounds(b,{padding:[25,25]});' +
    'function esc(s){return String(s||"")' +
    '.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")}' +
    '<\\/script></body></html>';

  // Escape pour attribut srcdoc HTML
  return html.replace(/"/g, "&quot;");
}

// =============================================================================
// TABLEAU HTML
// =============================================================================

function _genererTableau(concurrents) {
  var CAT_ORDER  = ["Personnes dependantes","Menage / Repassage","Garde d'enfants","Jardinage / Bricolage"];
  var CAT_COLORS = {
    "Personnes dependantes": "#e74c3c",
    "Menage / Repassage":    "#3498db",
    "Garde d'enfants":       "#2ecc71",
    "Jardinage / Bricolage": "#f39c12"
  };

  var grouped = {};
  concurrents.forEach(function(c) {
    var cat = c.categorie || "Autre";
    if (!grouped[cat]) grouped[cat] = [];
    grouped[cat].push(c);
  });

  var rows = "";
  var idx  = 0;
  CAT_ORDER.forEach(function(cat) {
    (grouped[cat] || []).forEach(function(c) {
      var bg    = idx++ % 2 === 0 ? "#fff" : "#f8fafc";
      var color = CAT_COLORS[cat] || "#888";
      rows +=
        '<tr style="background:' + bg + '">' +
        '<td style="padding:5px 10px;border-bottom:1px solid #eee">' + _esc(c.nom) + '</td>' +
        '<td style="padding:5px 10px;border-bottom:1px solid #eee">' + _esc(c.adresse) + '</td>' +
        '<td style="padding:5px 10px;border-bottom:1px solid #eee">' + _esc(c.ville || c.commune_scraped) + '</td>' +
        '<td style="padding:5px 10px;border-bottom:1px solid #eee">' + _esc(c.telephone) + '</td>' +
        '<td style="padding:5px 10px;border-bottom:1px solid #eee">' +
        '<span style="background:' + color + ';color:white;padding:2px 7px;' +
        'border-radius:9px;font-size:10px;font-weight:bold">' + _esc(cat) + '</span>' +
        '</td></tr>';
    });
  });

  return '<div style="margin-top:12px;font-family:Arial,sans-serif;font-size:11px;overflow-x:auto;">' +
    '<table style="width:100%;border-collapse:collapse;">' +
    '<thead><tr style="background:#1a3a5c;color:white">' +
    '<th style="padding:7px 10px;text-align:left">Raison sociale</th>' +
    '<th style="padding:7px 10px;text-align:left">Adresse</th>' +
    '<th style="padding:7px 10px;text-align:left">Commune</th>' +
    '<th style="padding:7px 10px;text-align:left">Telephone</th>' +
    '<th style="padding:7px 10px;text-align:left">Categorie</th>' +
    '</tr></thead>' +
    '<tbody>' + rows + '</tbody></table></div>';
}

// =============================================================================
// CACHE (PropertiesService — 30 jours)
// =============================================================================

function _getCacheResultat(communes, dpt) {
  try {
    var props = PropertiesService.getScriptProperties();
    var key   = "scraper_" + dpt + "_" +
      communes.slice(0, 3).map(function(c) { return c.replace(/\s/g, ""); }).join("_");
    var raw   = props.getProperty(key);
    if (!raw) return null;
    var cached = JSON.parse(raw);
    var age    = Date.now() - new Date(cached.meta.date_extraction).getTime();
    if (age > 30 * 24 * 3600 * 1000) return null;
    return cached;
  } catch(e) { return null; }
}

function _setCacheResultat(communes, dpt, result) {
  try {
    var props = PropertiesService.getScriptProperties();
    var key   = "scraper_" + dpt + "_" +
      communes.slice(0, 3).map(function(c) { return c.replace(/\s/g, ""); }).join("_");
    var json  = JSON.stringify({ meta: result.meta, concurrents: result.concurrents });
    if (json.length < 500000) {
      props.setProperty(key, json);
    } else {
      // Trop grand pour PropertiesService — sauvegarde dans Drive
      _sauvegarderDrive(result, communes, dpt);
    }
  } catch(e) { Logger.log("Cache non sauvegarde : " + e.message); }
}

function _sauvegarderDrive(result, communes, dpt) {
  try {
    var nom = "ELM_concurrents_" + communes[0].replace(/\s/g,"_") + "_" + dpt + "_" +
      new Date().toISOString().slice(0,10) + ".json";
    DriveApp.createFile(nom, JSON.stringify(result, null, 2), MimeType.PLAIN_TEXT);
    Logger.log("JSON sauvegarde dans Drive : " + nom);
  } catch(e) { Logger.log("Erreur sauvegarde Drive : " + e.message); }
}

// =============================================================================
// HTTP UTILS
// =============================================================================

function _get(path) {
  try {
    var resp = UrlFetchApp.fetch(SCRAPER_API_URL + path, {
      method: "get",
      muteHttpExceptions: true
    });
    return JSON.parse(resp.getContentText("utf-8"));
  } catch(e) {
    Logger.log("GET " + path + " erreur: " + e.message);
    return null;
  }
}

function _post(path, payload) {
  try {
    var resp = UrlFetchApp.fetch(SCRAPER_API_URL + path, {
      method:      "post",
      contentType: "application/json",
      payload:     JSON.stringify(payload),
      muteHttpExceptions: true
    });
    return JSON.parse(resp.getContentText("utf-8"));
  } catch(e) {
    Logger.log("POST " + path + " erreur: " + e.message);
    return null;
  }
}

function _ping() {
  var r = _get("/ping");
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
// FONCTIONS DE TEST — lancer depuis l'editeur Apps Script
// =============================================================================

/** Teste la connexion au serveur Render */
function testPing() {
  if (_ping()) {
    Logger.log("OK API accessible : " + SCRAPER_API_URL);
  } else {
    Logger.log("ERREUR API non accessible. Verifiez l'URL et que le service Render est actif.");
    Logger.log("Tip : le plan gratuit dort apres 15min — attendez 60s et reessayez.");
  }
}

/** Teste un scraping rapide sur 2 communes */
function testScrapingRapide() {
  var data = scraperConcurrents(["Saint-Lo", "Agneaux"], 50);
  Logger.log("Resultat : " + data.meta.total + " concurrents");
  Logger.log(JSON.stringify(data.meta.stats_par_categorie));
}

/** Vide le cache pour forcer un nouveau scraping */
function viderCache() {
  PropertiesService.getScriptProperties().deleteAllProperties();
  Logger.log("Cache vide.");
}
