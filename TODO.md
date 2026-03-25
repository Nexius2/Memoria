# TODO – Memoria

## Done / already implemented

- [x] Background jobs for detection / sync / expire
- [x] Dashboard overview with active events, ending soon, published targets, running jobs
- [x] Current settings snapshot widget on dashboard
- [x] Event status overview on dashboard
- [x] Problematic events summary on dashboard
- [x] Live control actions (run detection / sync / expire)
- [x] Manual person creation
- [x] People list with filters:
  - [x] search
  - [x] status
  - [x] source
  - [x] missing titles
  - [x] sorting
- [x] Bulk actions on people:
  - [x] pin
  - [x] unpin
  - [x] exclude
  - [x] unexclude
  - [x] ignore 30d
  - [x] clear ignore
  - [x] delete selected
- [x] Missing titles scan status stored on people
- [x] Missing titles counters displayed in people list
- [x] Person detail page
- [x] TMDb metadata refresh
- [x] TMDb photo display on person detail
- [x] Missing titles refresh from person detail
- [x] Arr integration basics
- [x] Arr “Test & Fill” flow
- [x] Root folder discovery
- [x] Quality profile discovery
- [x] Language profile discovery
- [x] Logs page with:
  - [x] source filter
  - [x] level filter
  - [x] date from/to
  - [x] limit
  - [x] expandable long messages
- [x] LocalStorage persistence for people filters
- [x] Auto-submit filters UI improvement
- [x] Cleaner bulk actions UI block
- [x] Better centered status badges in people table

---

## High priority / next evolutions

- [ ] Manual TMDb match resolution UI
  - [ ] Show best TMDb candidates when automatic matching is uncertain
  - [ ] Allow manual selection of the correct TMDb person
  - [ ] Store manual TMDb override on the person
  - [ ] Add “Re-match TMDb” action

- [ ] Global Missing Titles page
  - [ ] Show all missing movies / series across people
  - [ ] Filter by type (movie / show)
  - [ ] Filter by Arr readiness
  - [ ] Filter by person / country / source
  - [ ] Quick actions to send to Radarr / Sonarr

- [ ] Inline quick actions in People list
  - [ ] Refresh metadata directly from row
  - [ ] Refresh missing titles directly from row
  - [ ] Pin / exclude directly from row where useful

---

## Arr improvements

- [ ] Better visibility of Arr status per missing title
- [ ] Show whether a title was already sent to Arr
- [ ] Show whether title exists already in Radarr / Sonarr
- [ ] Prevent duplicate sends more explicitly in UI
- [ ] Add per-title manual “Send to Arr” action from detail views
- [ ] Add bulk “Send selected missing titles” flow

---

## TMDb / matching improvements

- [ ] Improve fallback matching when name is approximate
- [ ] Better homonym handling
- [ ] Better confidence explanation in UI
- [ ] Show why automatic TMDb match was rejected
- [ ] Manual override history / flag visibility improvements

---

## Dashboard improvements

- [ ] Missing titles widgets on dashboard
  - [ ] total missing movies
  - [ ] total missing shows
  - [ ] Arr-ready missing titles
  - [ ] people with most missing titles

- [ ] Surface candidates needing manual review
- [ ] Surface people with failed scans
- [ ] Surface excluded / ignored counts if useful

---

## UI / UX polish

- [ ] Replace bulk action buttons with a cleaner single action selector + apply button
- [ ] Improve table density / readability on People page
- [ ] Add clearer separation between filters and data actions across the app
- [ ] Add consistent iconography where useful
- [ ] Review spacing and alignment on all list/table pages
- [ ] Make reset/filter micro-interactions consistent across pages

---

## Settings / security

- [ ] Hide / mask Plex API keys in UI
- [ ] Hide / mask Arr API keys in UI
- [ ] Add explicit reveal / copy action for secret fields if needed

---

## Nice to have

- [ ] Global review queue for people needing manual attention
- [ ] Better audit trail for actions taken on people
- [ ] More helpful empty states in pages with no data
- [ ] Optional export of people / events / missing titles
- [ ] Better statistics around collection publishing