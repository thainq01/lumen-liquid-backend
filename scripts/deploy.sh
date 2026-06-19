#!/usr/bin/env bash
# ============================================================
# LumenLiquid Backend — Production Deploy Script
# ============================================================
# Pulls the latest image from GHCR, tears down old containers,
# and brings the stack back up with the new build.
#
# Usage:
#   ./scripts/deploy.sh            # standard deploy (preserves data)
#   ./scripts/deploy.sh --reset    # wipe DB/Redis volumes (destructive!)
#   ./scripts/deploy.sh --no-pull  # skip image pull (use local image)
#
# Prereqs on the server:
#   - .env present and filled (copy .env.prod.example → .env)
#   - logged into GHCR if the image is private:
#       echo "$GHCR_TOKEN" | docker login ghcr.io -u thainq01 --password-stdin
#   - dokploy-network exists (Dokploy's Traefik)
# ============================================================
set -euo pipefail

# ── Config ──────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.prod.yml"
COMPOSE=(docker compose -f "$COMPOSE_FILE")

# ── Flags ───────────────────────────────────────────────────
RESET=false
PULL=true
for arg in "$@"; do
  case "$arg" in
    --reset)   RESET=true ;;
    --no-pull) PULL=false ;;
    -h|--help)
      sed -n '2,16p' "$0"; exit 0 ;;
    *) echo "Unknown flag: $arg" >&2; exit 1 ;;
  esac
done

cd "$PROJECT_DIR"

# ── Preflight ───────────────────────────────────────────────
echo "==> Preflight checks"
[[ -f "$PROJECT_DIR/.env" ]] || { echo "ERROR: .env not found. Copy .env.prod.example → .env and fill values." >&2; exit 1; }
command -v docker >/dev/null || { echo "ERROR: docker not installed." >&2; exit 1; }

# Ensure the external Dokploy network exists (Traefik routes through it).
if ! docker network inspect dokploy-network >/dev/null 2>&1; then
  echo "ERROR: docker network 'dokploy-network' not found. Create it (or start Dokploy) first." >&2
  exit 1
fi

IMAGE_TAG="${IMAGE_TAG:-latest}"
echo "    compose file : $COMPOSE_FILE"
echo "    image tag    : ${IMAGE_TAG}"
echo "    reset data   : $RESET"

# ── Pull latest image ───────────────────────────────────────
if $PULL; then
  echo "==> Pulling latest image (ghcr.io/thainq01/lumen-liquid-backend:${IMAGE_TAG})"
  "${COMPOSE[@]}" pull
else
  echo "==> Skipping pull (--no-pull)"
fi

# ── Tear down old containers ────────────────────────────────
echo "==> Stopping and removing old containers"
if $RESET; then
  echo "    ⚠️  --reset: removing volumes (pgdata, redisdata) — ALL DB DATA WILL BE LOST"
  "${COMPOSE[@]}" down -v
else
  "${COMPOSE[@]}" down
fi

# ── Prune dangling images ───────────────────────────────────
echo "==> Pruning dangling images"
docker image prune -f

# ── Bring up the stack ──────────────────────────────────────
echo "==> Starting stack"
"${COMPOSE[@]}" up -d

# ── Status ──────────────────────────────────────────────────
echo "==> Container status"
"${COMPOSE[@]}" ps

echo
echo "==> Deploy complete. Tailing logs (Ctrl-C to stop)..."
echo
"${COMPOSE[@]}" logs -f
