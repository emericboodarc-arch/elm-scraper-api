// =============================================================================
// ELM_Scraper.gs
// Intégration de l'API scraper Pages Jaunes dans Apps Script
// =============================================================================
// SETUP : une seule chose à faire → remplacer l'URL ci-dessous
// par l'URL de ton service Render après déploiement.
// =============================================================================


const SCRAPER_API_URL = "https://elm-scraper-api-1.onrender.com"; // ← ton URL Render


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
