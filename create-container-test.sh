#!/bin/bash
set -euo pipefail

echo ""
echo "🚀 Déploiement du conteneur MEMORIA TEST"
echo ""

CONTAINER_NAME="memoria-test"
IMAGE_NAME="memoria:latest"

HOST_PORT="5011"
CONTAINER_PORT="8080"

APPDATA_PATH="/mnt/user/appdata/memoria-test"
BACKUP_PATH="/mnt/user/appdata/memoria-test/backups"
LOGS_PATH="/mnt/user/appdata/memoria-test/logs"

INFO_FILE="INFO"
[ -f "$INFO_FILE" ] && rm -f "$INFO_FILE"

VERSION=$(date +"%y.%m.%d b%H%M")
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}' || true)
SQLITE3_VERSION=$(sqlite3 --version 2>/dev/null | awk '{print $1}' || true)

if [ -f "$(pwd)/Dockerfile" ]; then
  DOCKER_IMAGE=$(grep -i "^FROM" "$(pwd)/Dockerfile" | awk '{print $2}')
elif [ -f "$(pwd)/backend/Dockerfile" ]; then
  DOCKER_IMAGE=$(grep -i "^FROM" "$(pwd)/backend/Dockerfile" | awk '{print $2}')
else
  DOCKER_IMAGE="$IMAGE_NAME"
fi

cat <<EOF > "$INFO_FILE"
VERSION=$VERSION
TOOL=memoria
CREATED_BY=NeXius2
PYTHON=$PYTHON_VERSION
SQLITE=$SQLITE3_VERSION
CONTAINER=Docker
IMAGE=$DOCKER_IMAGE
EOF

echo "✅ Fichier INFO créé (${VERSION})"
echo

mkdir -p "$APPDATA_PATH" "$BACKUP_PATH" "$LOGS_PATH"

# --- Capture de l'image actuelle (si elle existe) ---
OLD_IMAGE_ID="$(docker images -q "$IMAGE_NAME" 2>/dev/null | head -n 1 || true)"

echo "📦 Construction de l'image Docker..."
docker build --pull --rm --force-rm -t "$IMAGE_NAME" .

NEW_IMAGE_ID="$(docker images -q "$IMAGE_NAME" 2>/dev/null | head -n 1 || true)"

echo "🛑 Suppression de l'ancien conteneur..."
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

echo "🚀 Création du nouveau conteneur..."
docker run -d \
  --name "$CONTAINER_NAME" \
  --hostname "$CONTAINER_NAME" \
  -p "${HOST_PORT}:${CONTAINER_PORT}" \
  --restart unless-stopped \
  -v "${APPDATA_PATH}:/appdata" \
  -v "${LOGS_PATH}:/logs" \
  -v "${BACKUP_PATH}:/backups" \
  -e DATABASE_PATH="/appdata/database.db" \
  --label "app.memoria.managed=true" \
  --label "net.unraid.docker.webui=http://[IP]:[PORT:${CONTAINER_PORT}]" \
  "$IMAGE_NAME"

# --- Nettoyage : supprime l'ancienne image devenue "orpheline" ---
if [ -n "${OLD_IMAGE_ID:-}" ] && [ -n "${NEW_IMAGE_ID:-}" ] && [ "$OLD_IMAGE_ID" != "$NEW_IMAGE_ID" ]; then
  echo "🧹 Suppression de l'ancienne image devenue orpheline: $OLD_IMAGE_ID"
  docker rmi "$OLD_IMAGE_ID" 2>/dev/null || true
fi

# --- Nettoyage safe : ne supprime que les images "dangling" (orphelines/untagged) ---
echo "🧹 Nettoyage des images dangling..."
docker image prune -f --filter "dangling=true" >/dev/null || true

echo ""
echo "🎉 MEMORIA TEST déployé avec succès !"
echo "👉 URL : http://[IP]:${HOST_PORT}"
echo ""