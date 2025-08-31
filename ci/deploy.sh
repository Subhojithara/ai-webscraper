#!/bin/bash

# K-Scrape Nexus Deployment Script
# Deploys the application to Kubernetes using Kustomize

set -euo pipefail

# Configuration
ENVIRONMENT="${ENVIRONMENT:-staging}"
NAMESPACE="${NAMESPACE:-k-scrape-nexus-$ENVIRONMENT}"
REGISTRY="${REGISTRY:-localhost:5000}"
PROJECT_NAME="k-scrape-nexus"
VERSION="${VERSION:-latest}"

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

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check kubectl
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl is not installed or not in PATH"
        exit 1
    fi
    
    # Check kustomize
    if ! command -v kustomize &> /dev/null; then
        log_error "kustomize is not installed or not in PATH"
        exit 1
    fi
    
    # Check if we can connect to Kubernetes
    if ! kubectl cluster-info &> /dev/null; then
        log_error "Cannot connect to Kubernetes cluster"
        exit 1
    fi
    
    # Check if we're in the project root
    if [[ ! -f "pyproject.toml" ]] || [[ ! -d "kubernetes" ]]; then
        log_error "Please run this script from the project root directory"
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

# Validate environment
validate_environment() {
    local env=$1
    
    case $env in
        staging|production)
            log_info "Deploying to $env environment"
            ;;
        *)
            log_error "Invalid environment: $env. Must be 'staging' or 'production'"
            exit 1
            ;;
    esac
    
    if [[ ! -d "kubernetes/overlays/$env" ]]; then
        log_error "Environment overlay not found: kubernetes/overlays/$env"
        exit 1
    fi
}

# Create namespace if it doesn't exist
create_namespace() {
    local namespace=$1
    
    if kubectl get namespace "$namespace" &> /dev/null; then
        log_info "Namespace $namespace already exists"
    else
        log_info "Creating namespace $namespace..."
        kubectl create namespace "$namespace"
        kubectl label namespace "$namespace" app.kubernetes.io/name=k-scrape-nexus
        log_success "Namespace $namespace created"
    fi
}

# Apply SOPS secrets (if available)
apply_secrets() {
    local env=$1
    
    if command -v sops &> /dev/null; then
        log_info "Applying SOPS encrypted secrets..."
        
        # Decrypt and apply secrets
        sops -d kubernetes/base/shared/secrets.sops.yaml | kubectl apply -f -
        
        log_success "Secrets applied"
    else
        log_warning "SOPS not found, skipping encrypted secrets"
        log_warning "Make sure to manually apply secrets before deployment"
    fi
}

# Update image tags
update_image_tags() {
    local env=$1
    local version=$2
    local overlay_dir="kubernetes/overlays/$env"
    
    log_info "Updating image tags to $version..."
    
    # Services to update
    local services=(
        "api-server"
        "ingestion-service"
        "http-scraper-worker"
        "headless-scraper-worker"
        "data-processing-worker"
        "coordinator"
    )
    
    # Create a temporary kustomization with updated image tags
    local temp_kustomization=$(mktemp)
    cp "$overlay_dir/kustomization.yaml" "$temp_kustomization"
    
    for service in "${services[@]}"; do
        # Update image tag in kustomization
        sed -i "s|$PROJECT_NAME/$service:.*|$PROJECT_NAME/$service:$version|g" "$overlay_dir/kustomization.yaml"
    done
    
    log_success "Image tags updated"
}

# Deploy application
deploy_application() {
    local env=$1
    local namespace=$2
    
    log_info "Deploying K-Scrape Nexus to $env environment..."
    
    # Build and apply manifests
    kustomize build "kubernetes/overlays/$env" | kubectl apply -f -
    
    log_success "Application deployed"
}

# Wait for deployment
wait_for_deployment() {
    local namespace=$1
    
    log_info "Waiting for deployments to be ready..."
    
    # Get all deployments in the namespace
    local deployments=$(kubectl get deployments -n "$namespace" -o name | sed 's/deployment.apps\///')
    
    for deployment in $deployments; do
        log_info "Waiting for deployment $deployment..."
        kubectl rollout status deployment/"$deployment" -n "$namespace" --timeout=600s
    done
    
    log_success "All deployments are ready"
}

# Verify deployment
verify_deployment() {
    local namespace=$1
    
    log_info "Verifying deployment..."
    
    # Check pod status
    log_info "Pod status:"
    kubectl get pods -n "$namespace" -o wide
    
    # Check service status
    log_info "Service status:"
    kubectl get services -n "$namespace"
    
    # Check HPA status
    log_info "HPA status:"
    kubectl get hpa -n "$namespace" || log_warning "No HPA found"
    
    # Check KEDA ScaledObjects
    log_info "KEDA ScaledObjects:"
    kubectl get scaledobjects -n "$namespace" || log_warning "No ScaledObjects found"
    
    # Check for failed pods
    local failed_pods=$(kubectl get pods -n "$namespace" --field-selector=status.phase=Failed -o name | wc -l)
    if [[ $failed_pods -gt 0 ]]; then
        log_warning "Found $failed_pods failed pods"
        kubectl get pods -n "$namespace" --field-selector=status.phase=Failed
    fi
    
    log_success "Deployment verification completed"
}

# Rollback deployment
rollback_deployment() {
    local namespace=$1
    
    log_warning "Rolling back deployment..."
    
    # Get all deployments and rollback
    local deployments=$(kubectl get deployments -n "$namespace" -o name | sed 's/deployment.apps\///')
    
    for deployment in $deployments; do
        log_info "Rolling back deployment $deployment..."
        kubectl rollout undo deployment/"$deployment" -n "$namespace"
    done
    
    # Wait for rollback to complete
    wait_for_deployment "$namespace"
    
    log_success "Rollback completed"
}

# Clean up resources
cleanup_resources() {
    local namespace=$1
    
    log_warning "Cleaning up resources in namespace $namespace..."
    
    # Delete all resources in the namespace
    kubectl delete all --all -n "$namespace"
    
    # Delete the namespace
    kubectl delete namespace "$namespace"
    
    log_success "Cleanup completed"
}

# Health check
health_check() {
    local namespace=$1
    
    log_info "Running health checks..."
    
    # Check API server health
    local api_service=$(kubectl get service -n "$namespace" -l app.kubernetes.io/component=api-server -o name | head -1)
    if [[ -n "$api_service" ]]; then
        log_info "Checking API server health..."
        kubectl port-forward -n "$namespace" "$api_service" 8080:80 &
        local port_forward_pid=$!
        
        sleep 5
        
        if curl -f http://localhost:8080/health &> /dev/null; then
            log_success "API server health check passed"
        else
            log_warning "API server health check failed"
        fi
        
        kill $port_forward_pid 2>/dev/null || true
    fi
    
    log_success "Health checks completed"
}

# Main deployment process
main() {
    log_info "Starting K-Scrape Nexus deployment..."
    log_info "Environment: $ENVIRONMENT"
    log_info "Namespace: $NAMESPACE"
    log_info "Version: $VERSION"
    
    # Run checks
    check_prerequisites
    validate_environment "$ENVIRONMENT"
    
    # Create namespace
    create_namespace "$NAMESPACE"
    
    # Apply secrets (if available)
    apply_secrets "$ENVIRONMENT"
    
    # Update image tags
    update_image_tags "$ENVIRONMENT" "$VERSION"
    
    # Deploy application
    deploy_application "$ENVIRONMENT" "$NAMESPACE"
    
    # Wait for deployment
    wait_for_deployment "$NAMESPACE"
    
    # Verify deployment
    verify_deployment "$NAMESPACE"
    
    # Run health checks
    health_check "$NAMESPACE"
    
    log_success "Deployment completed successfully!"
    log_info "Access the API at: kubectl port-forward -n $NAMESPACE service/api-server-external 8080:80"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --environment|-e)
            ENVIRONMENT="$2"
            NAMESPACE="k-scrape-nexus-$ENVIRONMENT"
            shift 2
            ;;
        --namespace|-n)
            NAMESPACE="$2"
            shift 2
            ;;
        --version|-v)
            VERSION="$2"
            shift 2
            ;;
        --registry|-r)
            REGISTRY="$2"
            shift 2
            ;;
        --rollback)
            check_prerequisites
            rollback_deployment "$NAMESPACE"
            exit 0
            ;;
        --cleanup)
            check_prerequisites
            cleanup_resources "$NAMESPACE"
            exit 0
            ;;
        --verify)
            check_prerequisites
            verify_deployment "$NAMESPACE"
            exit 0
            ;;
        --health-check)
            check_prerequisites
            health_check "$NAMESPACE"
            exit 0
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -e, --environment ENV    Target environment (staging/production)"
            echo "  -n, --namespace NS       Kubernetes namespace"
            echo "  -v, --version VERSION    Image version tag"
            echo "  -r, --registry REGISTRY  Docker registry URL"
            echo "  --rollback              Rollback to previous deployment"
            echo "  --cleanup               Clean up all resources"
            echo "  --verify                Verify current deployment"
            echo "  --health-check          Run health checks"
            echo "  --help                  Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  ENVIRONMENT             Same as --environment"
            echo "  NAMESPACE               Same as --namespace"
            echo "  VERSION                 Same as --version"
            echo "  REGISTRY                Same as --registry"
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