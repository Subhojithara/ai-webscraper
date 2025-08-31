#!/bin/bash

# K-Scrape Nexus Docker Image Build Script
# Builds and pushes all service Docker images

set -euo pipefail

# Configuration
REGISTRY="${REGISTRY:-localhost:5000}"
PROJECT_NAME="k-scrape-nexus"
VERSION="${VERSION:-$(git rev-parse --short HEAD)}"
BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
GIT_COMMIT=$(git rev-parse HEAD)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Service definitions
declare -A services=(
    ["api-server"]="services/api-server"
    ["ingestion-service"]="services/ingestion-service"
    ["http-scraper-worker"]="services/http-scraper-worker"
    ["headless-scraper-worker"]="services/headless-scraper-worker"
    ["data-processing-worker"]="services/data-processing-worker"
    ["coordinator"]="services/coordinator"
)

# Build labels
build_labels() {
    echo "--label org.opencontainers.image.title=$PROJECT_NAME"
    echo "--label org.opencontainers.image.description='Enterprise-grade web scraping platform'"
    echo "--label org.opencontainers.image.version=$VERSION"
    echo "--label org.opencontainers.image.created=$BUILD_DATE"
    echo "--label org.opencontainers.image.revision=$GIT_COMMIT"
    echo "--label org.opencontainers.image.vendor='K-Scrape Nexus Team'"
    echo "--label org.opencontainers.image.licenses=MIT"
    echo "--label org.opencontainers.image.source=https://github.com/k-scrape-nexus/k-scrape-nexus"
}

# Build shared library base image
build_shared_lib() {
    log_info "Building shared library base image..."
    
    docker build \
        $(build_labels) \
        --tag "$REGISTRY/$PROJECT_NAME/shared-lib:$VERSION" \
        --tag "$REGISTRY/$PROJECT_NAME/shared-lib:latest" \
        --file services/worker-shared/Dockerfile \
        services/worker-shared/
    
    log_success "Shared library base image built"
}

# Build service image
build_service() {
    local service_name=$1
    local service_path=$2
    
    log_info "Building $service_name..."
    
    # Check if Dockerfile exists
    if [[ ! -f "$service_path/Dockerfile" ]]; then
        log_error "Dockerfile not found for $service_name at $service_path/Dockerfile"
        return 1
    fi
    
    # Build the image
    docker build \
        $(build_labels) \
        --label org.opencontainers.image.title="$PROJECT_NAME-$service_name" \
        --tag "$REGISTRY/$PROJECT_NAME/$service_name:$VERSION" \
        --tag "$REGISTRY/$PROJECT_NAME/$service_name:latest" \
        --file "$service_path/Dockerfile" \
        "$service_path/"
    
    log_success "$service_name image built"
}

# Push image to registry
push_image() {
    local image_name=$1
    
    log_info "Pushing $image_name..."
    
    docker push "$REGISTRY/$PROJECT_NAME/$image_name:$VERSION"
    docker push "$REGISTRY/$PROJECT_NAME/$image_name:latest"
    
    log_success "$image_name pushed to registry"
}

# Security scan with Trivy (if available)
security_scan() {
    local image_name=$1
    
    if command -v trivy &> /dev/null; then
        log_info "Running security scan for $image_name..."
        
        trivy image \
            --exit-code 1 \
            --severity HIGH,CRITICAL \
            --no-progress \
            "$REGISTRY/$PROJECT_NAME/$image_name:$VERSION"
        
        if [[ $? -eq 0 ]]; then
            log_success "Security scan passed for $image_name"
        else
            log_warning "Security scan found issues in $image_name"
        fi
    else
        log_warning "Trivy not found, skipping security scan"
    fi
}

# Main build process
main() {
    log_info "Starting Docker image build process..."
    log_info "Registry: $REGISTRY"
    log_info "Version: $VERSION"
    log_info "Build Date: $BUILD_DATE"
    log_info "Git Commit: $GIT_COMMIT"
    
    # Check Docker is available
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    # Check if we're in the project root
    if [[ ! -f "pyproject.toml" ]] || [[ ! -d "services" ]]; then
        log_error "Please run this script from the project root directory"
        exit 1
    fi
    
    # Build shared library first
    build_shared_lib
    
    # Build all services
    local failed_builds=()
    
    for service_name in "${!services[@]}"; do
        service_path="${services[$service_name]}"
        
        if build_service "$service_name" "$service_path"; then
            log_success "✓ $service_name built successfully"
        else
            log_error "✗ Failed to build $service_name"
            failed_builds+=("$service_name")
        fi
    done
    
    # Report build results
    if [[ ${#failed_builds[@]} -eq 0 ]]; then
        log_success "All images built successfully!"
    else
        log_error "Failed to build: ${failed_builds[*]}"
        exit 1
    fi
    
    # Push images if PUSH=true
    if [[ "${PUSH:-false}" == "true" ]]; then
        log_info "Pushing images to registry..."
        
        # Push shared lib
        push_image "shared-lib"
        
        # Push all services
        for service_name in "${!services[@]}"; do
            push_image "$service_name"
        done
        
        log_success "All images pushed successfully!"
    else
        log_info "Images built locally. Set PUSH=true to push to registry."
    fi
    
    # Run security scans if SCAN=true
    if [[ "${SCAN:-false}" == "true" ]]; then
        log_info "Running security scans..."
        
        for service_name in "${!services[@]}"; do
            security_scan "$service_name"
        done
        
        log_success "Security scans completed!"
    fi
    
    # Output summary
    log_success "Build process completed!"
    log_info "Built images:"
    
    echo "  - $REGISTRY/$PROJECT_NAME/shared-lib:$VERSION"
    for service_name in "${!services[@]}"; do
        echo "  - $REGISTRY/$PROJECT_NAME/$service_name:$VERSION"
    done
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --registry)
            REGISTRY="$2"
            shift 2
            ;;
        --version)
            VERSION="$2"
            shift 2
            ;;
        --push)
            PUSH=true
            shift
            ;;
        --scan)
            SCAN=true
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --registry REGISTRY   Docker registry URL (default: localhost:5000)"
            echo "  --version VERSION     Image version tag (default: git commit hash)"
            echo "  --push               Push images to registry after building"
            echo "  --scan               Run security scans with Trivy"
            echo "  --help               Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  REGISTRY             Same as --registry"
            echo "  VERSION              Same as --version"
            echo "  PUSH                 Same as --push"
            echo "  SCAN                 Same as --scan"
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Run main function
main "$@"