# Memoria

Memoria is a self-hosted application that automatically detects recently deceased actors and public figures, and creates curated collections in your Plex libraries.

The goal is simple: keep your media library up to date with meaningful "In Memoriam" collections — fully automated.

---

## ✨ Features

- Detect recently deceased people (actors, directors, etc.)
- Automatically match them with your Plex library content
- Create and manage Plex collections per library (Movies / TV Shows)
- Use the person’s photo as the collection poster
- Fully automated background jobs
- Configurable retention (auto-remove after X days)
- Manual addition of people (with automatic metadata correction)
- Integration with TMDB for accurate data

---

## 🧠 How It Works

1. Memoria fetches recently deceased people from external sources  
2. It enriches data using TMDB (names, images, metadata)  
3. It scans your Plex libraries to find matching media  
4. It creates or updates collections in Plex:
   - One collection per person  
   - One per library (Movies / TV Shows)  
5. After a configurable delay, collections are automatically removed  

---

## 📦 Requirements

- Python 3.10+
- Plex Media Server
- TMDB API key

---

## ⚙️ Installation

### 1. Clone the repository

git clone https://github.com/yourusername/memoria.git
cd memoria

### 2. Install dependencies

pip install -r requirements.txt

### 3. Configure environment

Create a .env file:

PLEX_URL=http://your-plex:32400  
PLEX_TOKEN=your_plex_token  

TMDB_API_KEY=your_tmdb_key  

### 4. Run the application

python run.py

---

## 🧩 Configuration

From the UI, you can:

- Configure Plex server(s)
- Set retention duration (how long collections stay)
- Enable/disable automation
- Add people manually
- Monitor jobs and logs

---

## 🔄 Automation

Memoria runs background jobs to:

- Fetch new deceased people
- Match content in Plex
- Create/update collections
- Clean expired collections

No manual action required once configured.

---

## 📁 Project Structure

memoria/
├── app/
├── templates/
├── static/
├── jobs/
├── utils/
├── run.py
└── requirements.txt

---

## ⚠️ Notes

- Collections are only created if matching media exists in your Plex libraries  
- Posters are fetched from TMDB  
- No modification is made to your existing media files  

---

## 🚀 Roadmap

- Jellyfin support  
- Better matching algorithm  
- Multi-language support  
- Performance improvements on large libraries  

---

## 🤝 Contributing

Contributions are welcome!  
Feel free to open issues or submit pull requests.

---

## 📜 License

MIT License