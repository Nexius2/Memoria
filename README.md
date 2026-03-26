# Memoria

Memoria is a self-hosted tool designed to automatically build and maintain **Plex collections based on people (actors, directors, etc.)**, and enrich your media ecosystem by detecting and adding missing content.

It connects your Plex server with TMDB and your *Arr stack (Radarr / Sonarr)* to keep everything up-to-date — without manual work.

---

## Features

- 🎬 Automatic Plex collections based on people  
- 🔍 Detect missing movies and TV shows  
- ➕ Send missing content to Radarr / Sonarr  
- 🔁 Continuous background synchronization  
- 🧠 Smart matching (handles duplicates and edge cases)  
- 📊 Simple web interface to monitor and control everything  

---

## Requirements

- Docker  
- Plex server  
- TMDB API key  
- (Optional but recommended)
  - Radarr
  - Sonarr

---

## Installation (Recommended)

The easiest way to install Memoria is to use the provided script:

### 1. Download the project

git clone https://github.com/YOUR_REPO/memoria.git  
cd memoria  

### 2. Run the container creation script

./create-container-test.sh  

That’s it.  
The script will:

- Build the Docker image  
- Create and start the container  
- Apply default configuration  

No manual environment variables required.

---

## Access the Web Interface

Once the container is running, open:

http://YOUR_SERVER_IP:PORT  

(Port depends on your container configuration — check your script output or Docker logs.)

---

## First Configuration

When you access Memoria for the first time:

1. Add your Plex server  
2. Add your TMDB API key  
3. (Optional) Configure Radarr / Sonarr  

Once configured, Memoria will start working automatically.

---

## How It Works

Memoria continuously:

1. Scans your Plex libraries  
2. Detects people (actors, directors, etc.)  
3. Creates and updates Plex collections  
4. Searches for missing content via TMDB  
5. Sends missing items to Radarr / Sonarr  

Everything runs in background jobs — no manual actions required.

---

## Automation

Memoria is designed to be **fully automatic**:

- Background tasks run continuously  
- Collections are updated automatically  
- Missing content is detected and handled  
- No cron setup required  

---

## Updating

To update Memoria:

git pull  
./create-container-test.sh  

The container will be rebuilt with the latest version.

---

## Troubleshooting

If something doesn’t work:

- Check container logs  
docker logs memoria  

- Verify:
  - Plex connection  
  - TMDB API key  
  - Arr services availability  

---

## Roadmap

- Improved matching accuracy  
- Better UI monitoring  
- Advanced filtering and rules  
- Performance optimizations  

---

## Contributing

Contributions are welcome.

If you find bugs or want to improve the project, feel free to open an issue or submit a pull request.

---

## License

This project is open-source.  
License details will be added soon.