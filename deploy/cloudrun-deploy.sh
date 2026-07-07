#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
#  Deploy Aegis-Storage to Google Cloud Run (free tier eligible)
#
#  What this does:
#    1. Builds the container image with Cloud Build (no local Docker needed)
#    2. Pushes it to Artifact Registry
#    3. Deploys it to Cloud Run, running in `serve` mode (the HTTP wrapper),
#       listening on Cloud Run's injected $PORT, scaling to zero when idle.
#
#  Cost: Cloud Run's always-free tier includes 2,000,000 requests/month,
#  360,000 GB-seconds and 180,000 vCPU-seconds of compute, and 1GB egress —
#  a demo/portfolio deployment like this one stays inside that for free.
#  Storage is ephemeral (container-local disk): each new revision or scale-to-
#  zero cold start starts with an empty CommitLog/MemTable/SSTables. That's a
#  correct reflection of what this project is (a from-scratch storage engine
#  demo), not a place to keep data you care about — see the README section
#  "Persistence on Cloud Run" for how to change that if you want it.
#
#  Prerequisites:
#    - gcloud CLI installed and authenticated: `gcloud auth login`
#    - A GCP project with billing enabled (required to enable the APIs below,
#      even though this deployment itself should stay within the free tier)
#
#  Usage:
#    ./deploy/cloudrun-deploy.sh YOUR_GCP_PROJECT_ID [REGION]
#
#    REGION defaults to us-central1 (one of the regions with the lowest
#    Cloud Run free-tier-eligible pricing).
# ═══════════════════════════════════════════════════════════════════════════════
set -euo pipefail

PROJECT_ID="${1:?Usage: ./deploy/cloudrun-deploy.sh YOUR_GCP_PROJECT_ID [REGION]}"
REGION="${2:-us-central1}"
SERVICE="aegis-storage"
REPO="aegis-storage-repo"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE}"

echo "==> Using project: ${PROJECT_ID}  region: ${REGION}"
gcloud config set project "${PROJECT_ID}" -q

echo "==> Enabling required APIs (idempotent)..."
gcloud services enable \
    run.googleapis.com \
    artifactregistry.googleapis.com \
    cloudbuild.googleapis.com -q

echo "==> Ensuring Artifact Registry repo exists..."
gcloud artifacts repositories describe "${REPO}" --location="${REGION}" >/dev/null 2>&1 || \
gcloud artifacts repositories create "${REPO}" \
    --repository-format=docker \
    --location="${REGION}" \
    --description="Aegis-Storage images"

echo "==> Building image with Cloud Build..."
gcloud builds submit --tag "${IMAGE}:latest" "$(dirname "$0")/.."

echo "==> Deploying to Cloud Run..."
gcloud run deploy "${SERVICE}" \
    --image "${IMAGE}:latest" \
    --region "${REGION}" \
    --platform managed \
    --allow-unauthenticated \
    --port 8080 \
    --args "serve" \
    --memory 512Mi \
    --cpu 1 \
    --max-instances 2 \
    --min-instances 0

echo "==> Done. Service URL:"
gcloud run services describe "${SERVICE}" --region "${REGION}" --format='value(status.url)'
