# images.ps1
# Загрузка всех образов Observability стека в minikube
# Запускать из папки obs/ перед install.ps1

Write-Host "=== Pulling images from Docker Hub ===" -ForegroundColor Cyan

$images = @(
    # Prometheus stack
    "grafana/grafana:13.0.1-security-01",
    "quay.io/prometheus/prometheus:v3.11.3-distroless",
    "quay.io/prometheus/alertmanager:v0.32.1",
    "quay.io/prometheus-operator/prometheus-operator:v0.90.1",
    "quay.io/prometheus-operator/prometheus-config-reloader:v0.90.1",
    "registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.18.0",
    "quay.io/prometheus/node-exporter:v1.8.2",
    "quay.io/brancz/kube-rbac-proxy:v0.18.0",
    "quay.io/kiwigrid/k8s-sidecar:2.7.3",
    # Loki stack
    "grafana/loki:2.6.1",
    "grafana/promtail:3.5.1",
    # Jaeger
    "jaegertracing/all-in-one:1.57"
)

foreach ($img in $images) {
    Write-Host "Pulling $img ..." -ForegroundColor Yellow
    docker pull $img
}

Write-Host ""
Write-Host "=== Loading images into minikube ===" -ForegroundColor Cyan

foreach ($img in $images) {
    Write-Host "Loading $img ..." -ForegroundColor Yellow
    minikube image load $img
}

Write-Host ""
Write-Host "=== Done! All images loaded. ===" -ForegroundColor Green
