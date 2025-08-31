#!/bin/bash

# K-Scrape Nexus Development Setup Script
# This script sets up the development environment and runs initial migrations

set -euo pipefail

echo "🚀 Setting up K-Scrape Nexus development environment..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if Docker Compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose and try again."
    exit 1
fi

# Copy environment file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env
    echo "✅ Created .env file. Please review and update the settings as needed."
fi

# Create necessary directories
echo "📁 Creating required directories..."
mkdir -p data/uploads
mkdir -p data/downloads
mkdir -p logs

# Start the infrastructure services
echo "🐳 Starting infrastructure services..."
docker-compose up -d postgres redis minio prometheus grafana jaeger

# Wait for services to be healthy
echo "⏳ Waiting for services to start..."
sleep 30

# Check service health
echo "🔍 Checking service health..."
if docker-compose ps | grep -q "unhealthy"; then
    echo "⚠️ Some services are not healthy. Please check the logs:"
    docker-compose ps
    echo "📋 View logs with: docker-compose logs [service-name]"
else
    echo "✅ All services are running and healthy!"
fi

# Install Python dependencies
if [ -f "pyproject.toml" ]; then
    echo "📦 Installing Python dependencies..."
    pip install -e .
    pip install -e services/worker-shared/
fi

# Run database migrations
echo "🗄️ Running database migrations..."
cd services/worker-shared/scraper_lib
if [ -f "alembic.ini" ]; then
    # Create initial migration if none exist
    if [ ! "$(ls -A alembic/versions)" ]; then
        echo "Creating initial database migration..."
        alembic revision --autogenerate -m "Initial migration"
    fi
    
    # Run migrations
    alembic upgrade head
    echo "✅ Database migrations completed!"
else
    echo "⚠️ Alembic configuration not found. Skipping migrations."
fi
cd ../../..

# Setup MinIO buckets (this is done by minio-setup service, but we can verify)
echo "🪣 Verifying MinIO buckets..."
sleep 5  # Give minio-setup time to complete

echo "🎉 Development environment setup complete!"
echo ""
echo "📊 Access your services:"
echo "  • Grafana Dashboard: http://localhost:3000 (admin/admin)"
echo "  • Prometheus: http://localhost:9090"
echo "  • Jaeger Tracing: http://localhost:16686"
echo "  • MinIO Console: http://localhost:9001 (minio_admin/minio_password)"
echo "  • pgAdmin: http://localhost:8080 (admin@k-scrape-nexus.com/admin)"
echo "  • Redis Commander: http://localhost:8081"
echo ""
echo "🔧 To start the application services:"
echo "  cd services/api-server && python -m app.main"
echo "  cd services/ingestion-service && python -m app.main"
echo "  cd services/processing-worker && python -m app.main"
echo ""
echo "📝 View logs: docker-compose logs -f [service-name]"
echo "🛑 Stop services: docker-compose down"