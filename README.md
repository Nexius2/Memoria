# Memoria

Memoria is a self-hosted Docker application that automatically detects recently deceased actors and public figures, matches them against your Plex libraries, and creates curated **In Memoriam** collections.

The goal is simple: keep your Plex library up to date with meaningful memorial collections, with as little manual work as possible.

---

## Features

- Detect recently deceased people
- Enrich metadata with TMDb
- Match people against your Plex libraries
- Create Plex collections automatically
- Create one collection per person and per matching library
- Use the person's image as the collection poster
- Automatically remove expired collections after a configurable delay
- Add people manually
- Correct manually entered person data using fetched metadata
- Background jobs for automation
- Job history and logs from the web UI

---

## How it works

1. Memoria fetches recently deceased people from external sources
2. It enriches them with metadata from TMDb
3. It scans your Plex libraries
4. It looks for matching movies and TV shows in your media libraries
5. It creates or updates Plex collections for matching content
6. After the configured retention period, collections are removed automatically

Collections are only created when matching media exists in your Plex libraries.

---

## Important notes

- Memoria is designed to run with Docker / Docker Compose
- Configuration of Plex servers, TMDb, and Arr services is mainly done from the web UI
- The built-in scheduler is intended to run in a single application instance
- Do not run multiple Memoria containers with the scheduler enabled against the same database
- This project is intended for self-hosted/private use
- Do not expose it directly to the public Internet without proper protection

---

## Requirements

- Docker
- Docker Compose
- A Plex Media Server
- A TMDb API key

---

## Quick start

### 1. Clone the repository

git clone https://github.com/yourusername/memoria.git
cd memoria

### 2. Create the data folder

mkdir -p data

### 3. Edit docker-compose.yml

At minimum, change:

- SECRET_KEY
- optionally TZ

Example:

environment:
  TZ: Europe/Paris
  SECRET_KEY: change-me-with-a-long-random-secret
  DATABASE_PATH: /data/memoria.db
  SCHEDULER_ENABLED: "1"

Use a long random value for SECRET_KEY.

### 4. Start Memoria

docker compose up -d --build

### 5. Open the web UI

http://localhost:8080

---

## Docker configuration

The default Docker setup uses:

- port: 8080
- database path in container: /data/memoria.db
- mounted persistent folder: ./data

### Environment variables

| Variable | Required | Default | Description |
|---|---:|---|---|
| SECRET_KEY | Yes | change-me | Flask secret key. Change this before real use. |
| DATABASE_PATH | No | /data/memoria.db | SQLite database path inside the container |
| SCHEDULER_ENABLED | No | 1 | Enables background jobs |
| TZ | No | system default | Container timezone |

---

## First-time setup

After the container is running, use the web UI to configure:

- Plex server URL and token
- TMDb API key
- Collection retention settings
- Optional Arr integrations
- Automation behavior

Important: Plex, TMDb and Arr are not primarily configured from environment variables. They are configured from the application interface.

---

## Data persistence

Memoria stores its SQLite database in:

/data/memoria.db

With the provided Compose file, that maps to:

./data/memoria.db

If you remove the container but keep the data folder, your application data is preserved.

---

## Updating

To update after pulling new code:

git pull
docker compose up -d --build

---

## Security

Memoria is a self-hosted tool and should be treated as an internal/private application.

Recommended:

- put it behind a reverse proxy if needed
- restrict external access
- do not expose it publicly without protection
- always change the default SECRET_KEY
- avoid sharing screenshots that expose tokens or API keys

---

## Known limitations

- The scheduler is embedded in the application container
- Only one active Memoria instance should run against the same database when automation is enabled
- SQLite is used by default
- Large Plex libraries may result in longer scans and matching operations
- Some actions depend on the scheduler being enabled

---

## What Memoria does not do

- It does not modify your media files
- It does not delete your media
- It only creates, updates, or removes Plex collections based on matches found in your libraries

---

## Project status

Memoria is under active development. The core workflow is already usable, but improvements and refinements are still ongoing.

Planned and ongoing areas include:

- matching improvements
- UI polish
- scheduler and background task robustness
- better publication/readiness for public release

---

## Contributing

Contributions, feedback, and issue reports are welcome.

If you open an issue, include:

- your Memoria version
- your Docker setup
- logs
- steps to reproduce the issue

---

## License

MIT License

A LICENSE file should be included at the root of the repository.