#!/bin/sh

if [ -f .env ]; then
  echo "ℹ️  .env already exists. Leaving it unchanged."
  echo "   If you want defaults, remove or rename .env and run this script again."
  exit 0
fi

if [ ! -f .env.example ]; then
  echo "❌ .env.example not found in project root."
  exit 1
fi

cp .env.example .env

echo "✅ Created .env from .env.example with default credentials."
echo "   Review .env and update secrets before deploying to shared environments."
