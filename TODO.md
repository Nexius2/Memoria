# TODO – Memoria

Updated from the current codebase on 2026-04-07.
This file now focuses on what is still missing or worth improving, instead of keeping already-finished items mixed in.

## Recently completed / now in place

- [x] Recent Arr activity limited to the latest 10 rows
- [x] Previous / Next navigation for Recent Arr activity
- [x] Background jobs for detection / sync / expire
- [x] Dashboard overview with active events, ending soon, published targets, running jobs
- [x] Current settings snapshot widget on dashboard
- [x] Event status overview on dashboard
- [x] Problematic events summary on dashboard
- [x] Live control actions (run detection / sync / expire)
- [x] Manual person creation
- [x] People list filters (search / status / source / missing titles / sorting)
- [x] Bulk actions on people (pin / unpin / exclude / unexclude / ignore 30d / clear ignore / delete selected)
- [x] Missing titles scan status stored on people
- [x] Missing titles counters displayed in people list
- [x] Person detail page
- [x] TMDb metadata refresh
- [x] TMDb photo display on person detail
- [x] Manual TMDb candidate selection UI on person detail
- [x] “Re-match TMDb” action
- [x] Missing titles refresh from person detail
- [x] Arr integration basics
- [x] Arr “Test & Fill” flow
- [x] Root folder discovery
- [x] Quality profile discovery
- [x] Language profile discovery
- [x] Logs page with filters + CSV export
- [x] LocalStorage persistence for people filters
- [x] Auto-submit filters UI improvement
- [x] Cleaner bulk actions UI block
- [x] Better centered status badges in people table
- [x] Minimum people priority level saved in settings
- [x] Minimum people priority level applied consistently on dashboard candidates
- [x] Minimum people priority level applied consistently on People page for web-created people
- [x] Web candidate priority persisted on Person records

---

## High priority / still missing

- [x] Global Missing Titles page
  - [x] Show all missing movies / series across people
  - [x] Filter by type (movie / show)
  - [x] Filter by Arr readiness
  - [x] Filter by person / country / source
  - [x] Quick actions to send to Radarr / Sonarr

- [x] Better Arr visibility from missing titles / detail views
  - [x] Show whether a title was already sent to Arr
  - [x] Show whether a title already exists in Radarr / Sonarr
  - [x] Prevent duplicate sends more explicitly in UI
  - [x] Add per-title manual “Send to Arr” action from detail views
  - [x] Add bulk “Send selected missing titles” flow

- [x] Inline quick actions in People list
  - [x] Refresh metadata directly from row
  - [x] Refresh missing titles directly from row
  - [x] Pin / exclude directly from row where useful

- [x] Fix Plex library cache sorting when titles contain mixed year values (None / int)

---

## Matching / TMDb improvements

- [x] Improve fallback matching when name is approximate
- [x] Better homonym handling
- [x] Better confidence explanation in UI
- [x] Show why automatic TMDb match was rejected
- [x] Manual override history / flag visibility improvements

---

## Dashboard improvements

- [ ] Missing titles widgets on dashboard
  - [x] total missing movies
  - [x] total missing shows
  - [x] Arr-ready missing titles
  - [x] people with most missing titles

- [x] Surface candidates needing manual review
- [x] Surface people with failed scans
- [x] Surface excluded / ignored counts if useful

---

## UI / UX polish

- [x] Improve table density / readability on People page
- [x] Add clearer separation between filters and data actions across the app
- [x] Add consistent iconography where useful
- [x] Review spacing and alignment on all list/table pages
- [x] Make reset/filter micro-interactions consistent across pages
- [x] Show effective web priority more clearly in People / Dashboard UI
- [x] nettoyer le dashboard pour le rendre plus lisible / cacher les info non primaire via un toggle advanced
- [x] limiter le nombre de job visible (20)
- [x] limiter le nombre de logs visible (20)
- [x] dans settings, ne pas permettre de changer le app name
- [x] affichage de la version en bas a droite de la page via le fichier INFO
- [x] modifier les bouton du haut (dashboard, jobs, logs ....) en onglet plus presentable.
- [ ] controle du multilangue et rajout dans le json du manquant.
- [x] lenteur quand on clic sur arr

---

## Settings / security

- [x] Hide / mask Plex API keys in UI
- [x] Hide / mask Arr API keys in UI
- [x] Add explicit reveal / copy action for secret fields if needed

---

## Nice to have

- [x] Global review queue for people needing manual attention
- [ ] Better audit trail for actions taken on people
- [ ] More helpful empty states in pages with no data
- [ ] Optional export of people / events / missing titles
- [ ] Better statistics around collection publishing