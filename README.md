# K-Scrape Nexus

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-green.svg)](https://fastapi.tiangolo.com/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-1.25+-blue.svg)](https://kubernetes.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

K-Scrape Nexus is an enterprise-grade, hyper-scalable web scraping platform designed for massive workloads with complete automation and operational excellence. Built on cloud-native principles with Kubernetes orchestration, multi-layered autoscaling, and comprehensive observability.

## 🚀 Features

- **Hyper-Scalable Architecture**: Multi-layered autoscaling with KEDA, HPA, and Cluster Autoscaler
- **Asynchronous Processing**: Event-driven architecture with Redis Streams for queue management
- **Production-Ready**: GitOps deployment with ArgoCD, comprehensive monitoring, and zero-trust security
- **Multi-Protocol Scraping**: HTTP/HTTPS with connection pooling and headless browser support
- **Advanced Data Processing**: Pandas/Polars-based data cleaning and transformation pipelines
- **Enterprise Security**: Network policies, encrypted secrets with SOPS, RBAC
- **Full Observability**: Prometheus metrics, Grafana dashboards, Loki logs, Tempo traces

## 🏗️ Architecture

The system consists of microservices orchestrated by Kubernetes:

- **API Server**: FastAPI-based REST API for job submission and status monitoring
- **Ingestion Service**: File processing and URL extraction from uploaded files
- **HTTP Scraper Worker**: High-performance HTTP scraping with connection pooling
- **Headless Scraper Worker**: Playwright-based scraping for JavaScript-heavy sites
- **Data Processing Worker**: Pandas-based data cleaning and transformation
- **Job Coordinator**: Orchestration and state management across the pipeline

## 📋 Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Kubernetes cluster (for production)
- PostgreSQL
- Redis
- S3-compatible storage

## 🛠️ Quick Start

### Local Development

1. **Clone the repository**:
```bash
git clone <repository-url>
cd k-scrape-nexus
```

2. **Start development environment**:
```bash
docker-compose up -d
```

3. **Install dependencies**:
```bash
pip install -e .
```

4. **Run database migrations**:
```bash
cd services/worker-shared
alembic upgrade head
```

5. **Start services** (in separate terminals):
```bash
# API Server
cd services/api-server && python -m app.main

# Ingestion Service
cd services/ingestion-service && python -m app.main

# Workers
cd services/processing-worker && python -m app.main
```

### Production Deployment

See [docs/deployment.md](docs/deployment.md) for comprehensive Kubernetes deployment instructions.

## 📊 Monitoring & Observability

The platform includes a complete observability stack:

- **Metrics**: Prometheus with custom business metrics
- **Logs**: Structured JSON logging with Loki aggregation
- **Traces**: Distributed tracing with Tempo
- **Dashboards**: Pre-configured Grafana dashboards
- **Alerts**: Prometheus AlertManager rules

Access the monitoring stack at:
- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`

## 🔧 Configuration

Configuration is managed through environment variables and can be overridden for different environments:

- Development: `.env.development`
- Staging: `kubernetes/overlays/staging/`
- Production: `kubernetes/overlays/production/`

## 🧪 Testing

Run the comprehensive test suite:

```bash
# Unit tests
pytest services/*/tests/

# Integration tests
pytest tests/integration/

# End-to-end tests
pytest tests/e2e/
```

## 📖 Documentation

- [System Architecture](docs/product_design.md)
- [API Documentation](docs/api/)
- [Deployment Guide](docs/deployment/)
- [Development Guide](docs/development/)

## 🔒 Security

- Network segmentation with Kubernetes Network Policies
- Encrypted secrets management with SOPS
- Least-privilege RBAC policies
- Container security scanning in CI/CD

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🏢 Enterprise Support

For enterprise support, custom integrations, or consulting services, please contact our team.

---

**Built with ❤️ for enterprise-scale web scraping**