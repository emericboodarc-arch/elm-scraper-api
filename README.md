# ELM Scraper API

API REST qui scrape Pages Jaunes pour les ELM APEF.  
Déployée sur Render.com, appelée depuis Apps Script.

## Déploiement Render

1. Pusher ce repo sur GitHub
2. Aller sur [render.com](https://render.com) → New Web Service → connecter le repo
3. Render détecte `render.yaml` automatiquement
4. Copier l'URL du service (ex: `https://elm-scraper-api.onrender.com`)

## Routes

| Méthode | Route | Description |
|---------|-------|-------------|
| GET | `/ping` | Health check |
| POST | `/scrape` | Lance le scraping |
| GET | `/status` | Progression |
| GET | `/result` | Résultat complet |
| GET | `/result/summary` | Stats uniquement |

## Exemple d'appel

```json
POST /scrape
{
  "communes": ["Saint-Lo", "Agneaux", "Canisy"],
  "dpt": 50,
  "no_geocode": false
}
```
