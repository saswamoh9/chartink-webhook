#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# deploy.sh  —  Build, push, and deploy chartink-webhook to Cloud Run
#
# Prerequisites:
#   gcloud auth login && gcloud auth configure-docker
#   gcloud config set project YOUR_PROJECT_ID
#
# Usage:
#   ./deploy.sh                          # deploy with current gcloud project
#   PROJECT_ID=my-proj ./deploy.sh       # override project
# ---------------------------------------------------------------------------
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
REGION="asia-south1"
SERVICE="chartink-webhook"
IMAGE="gcr.io/${PROJECT_ID}/${SERVICE}"
SA="${SERVICE}-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo "==> Project : ${PROJECT_ID}"
echo "==> Region  : ${REGION}"
echo "==> Image   : ${IMAGE}:latest"
echo ""

# ── 1. Build ─────────────────────────────────────────────────────────────────
echo "==> Building Docker image..."
docker build -t "${IMAGE}:latest" ./app

# ── 2. Push ──────────────────────────────────────────────────────────────────
echo "==> Pushing to Container Registry..."
docker push "${IMAGE}:latest"

# ── 3. Deploy ────────────────────────────────────────────────────────────────
echo "==> Deploying to Cloud Run..."
gcloud run deploy "${SERVICE}" \
  --image="${IMAGE}:latest" \
  --region="${REGION}" \
  --platform=managed \
  --allow-unauthenticated \
  --memory=1Gi \
  --cpu=1 \
  --concurrency=10 \
  --timeout=300 \
  --min-instances=0 \
  --max-instances=1 \
  --no-cpu-throttling \
  --set-env-vars="GCP_PROJECT_ID=${PROJECT_ID},USE_SECRET_MANAGER=true" \
  --service-account="${SA}"

# ── 4. Print URL ─────────────────────────────────────────────────────────────
echo ""
echo "==> Deployed! Service URL:"
SERVICE_URL=$(gcloud run services describe "${SERVICE}" \
  --region="${REGION}" \
  --format="value(status.url)")
echo "    ${SERVICE_URL}"
echo ""
echo "==> Webhook routes:"
curl -s "${SERVICE_URL}/routes" | python3 -m json.tool
