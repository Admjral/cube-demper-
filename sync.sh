#!/bin/bash

# Cube Demper Sync Script
# Синхронизирует изменения между полным репо и отдельными репозиториями

set -e

FULL_REPO="/Users/adilhamitov/Desktop/cube-demper-full"
FRONTEND_REPO="${FRONTEND_REPO:-/Users/adilhamitov/Desktop/Cube Demper/frontend}"
BACKEND_REPO="${BACKEND_REPO:-/Users/adilhamitov/Desktop/Cube Demper/new-backend}"

# GitHub URLs for reference:
# Frontend: https://github.com/Admjral/Demper_front
# Backend: https://github.com/Admjral/cube-demper-

echo "=== Cube Demper Sync ==="

case "$1" in
  "pull-frontend")
    echo "Pulling frontend changes from $FRONTEND_REPO..."
    rsync -av --exclude='.git' --exclude='node_modules' --exclude='.next' \
      "$FRONTEND_REPO/" "$FULL_REPO/frontend/"
    cd "$FULL_REPO"
    git add frontend/
    git commit -m "Sync frontend from Demper_front" || echo "No changes to commit"
    ;;

  "pull-backend")
    echo "Pulling backend changes from $BACKEND_REPO..."
    rsync -av --exclude='.git' --exclude='__pycache__' --exclude='.venv' --exclude='venv' \
      "$BACKEND_REPO/" "$FULL_REPO/new-backend/"
    cd "$FULL_REPO"
    git add new-backend/
    git commit -m "Sync backend from cube-demper-" || echo "No changes to commit"
    ;;

  "push-frontend")
    echo "Pushing frontend changes to $FRONTEND_REPO..."
    rsync -av --delete --exclude='.git' --exclude='node_modules' --exclude='.next' \
      "$FULL_REPO/frontend/" "$FRONTEND_REPO/"
    cd "$FRONTEND_REPO"
    git add -A
    git commit -m "Sync from cube-demper-full" || echo "No changes to commit"
    git push origin master
    ;;

  "push-backend")
    echo "Pushing backend changes to $BACKEND_REPO..."
    rsync -av --delete --exclude='.git' --exclude='__pycache__' --exclude='.venv' --exclude='venv' \
      "$FULL_REPO/new-backend/" "$BACKEND_REPO/"
    cd "$BACKEND_REPO"
    git add -A
    git commit -m "Sync from cube-demper-full" || echo "No changes to commit"
    git push origin main
    ;;

  "pull-all")
    $0 pull-frontend
    $0 pull-backend
    ;;

  "push-all")
    $0 push-frontend
    $0 push-backend
    ;;

  *)
    echo "Usage: $0 {pull-frontend|pull-backend|push-frontend|push-backend|pull-all|push-all}"
    echo ""
    echo "Environment variables:"
    echo "  FRONTEND_REPO - path to Demper_front repo"
    echo "  BACKEND_REPO - path to cube-demper- repo"
    exit 1
    ;;
esac

echo "Done!"
