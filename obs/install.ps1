# install.ps1
# Установка полного Observability стека: Prometheus, Grafana, Loki, Jaeger
# Запускать из папки obs/ после images.ps1
# Требования: minikube запущен, helm установлен

# ---------------------------------------------------------------
# 1. Helm репозитории
# ---------------------------------------------------------------
Write-Host "=== Adding Helm repos ===" -ForegroundColor Cyan

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo add jaegertracing https://jaegertracing.github.io/helm-charts
helm repo update

# ---------------------------------------------------------------
# 2. Prometheus + Grafana
# ---------------------------------------------------------------
Write-Host ""
Write-Host "=== Installing Prometheus + Grafana ===" -ForegroundColor Cyan

helm install prometheus prometheus-community/kube-prometheus-stack `
  --namespace monitoring `
  --create-namespace `
  --set prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues=false `
  --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false `
  --set grafana.adminPassword=admin123 `
  --set grafana.image.pullPolicy=Never `
  --set prometheus.prometheusSpec.image.pullPolicy=Never `
  --set alertmanager.alertmanagerSpec.image.pullPolicy=Never `
  --set prometheusOperator.image.pullPolicy=Never `
  --set prometheusOperator.prometheusConfigReloader.image.pullPolicy=Never `
  --set kube-state-metrics.image.pullPolicy=Never `
  --set nodeExporter.image.pullPolicy=Never

Write-Host "Waiting for Prometheus stack pods..." -ForegroundColor Yellow
kubectl wait --for=condition=Ready pod `
  -l "app.kubernetes.io/instance=prometheus" `
  -n monitoring `
  --timeout=300s

# ---------------------------------------------------------------
# 3. Loki + Promtail
# ---------------------------------------------------------------
Write-Host ""
Write-Host "=== Installing Loki + Promtail ===" -ForegroundColor Cyan

helm install loki grafana/loki-stack `
  --namespace monitoring `
  --set loki.image.pullPolicy=Never `
  --set promtail.image.pullPolicy=Never `
  --set grafana.enabled=false `
  --set prometheus.enabled=false

Write-Host "Waiting for Loki pods..." -ForegroundColor Yellow
kubectl wait --for=condition=Ready pod `
  -l "app=loki" `
  -n monitoring `
  --timeout=120s

# ---------------------------------------------------------------
# 4. Jaeger
# ---------------------------------------------------------------
Write-Host ""
Write-Host "=== Installing Jaeger ===" -ForegroundColor Cyan

helm install jaeger jaegertracing/jaeger `
  --namespace monitoring `
  --set allInOne.enabled=true `
  --set allInOne.image.tag=1.57 `
  --set allInOne.image.pullPolicy=Never `
  --set provisionDataStore.cassandra=false `
  --set provisionDataStore.elasticsearch=false `
  --set storage.type=memory `
  --set agent.enabled=false `
  --set collector.enabled=false `
  --set query.enabled=false

Write-Host "Waiting for Jaeger pods..." -ForegroundColor Yellow
kubectl wait --for=condition=Ready pod `
  -l "app.kubernetes.io/instance=jaeger" `
  -n monitoring `
  --timeout=120s

# ---------------------------------------------------------------
# 5. Datasources в Grafana
# ---------------------------------------------------------------
Write-Host ""
Write-Host "=== Adding Grafana datasources ===" -ForegroundColor Cyan

# Получаем имя пода Grafana
$grafanaPod = kubectl get pod -n monitoring -l "app.kubernetes.io/name=grafana" -o jsonpath="{.items[0].metadata.name}"
Write-Host "Grafana pod: $grafanaPod" -ForegroundColor Yellow

# Ждём пока Grafana поднимется
Start-Sleep -Seconds 10

# Копируем и применяем datasources
kubectl cp "$PSScriptRoot\loki.json" "monitoring/${grafanaPod}:/tmp/loki.json" -c grafana
kubectl cp "$PSScriptRoot\jaeger.json" "monitoring/${grafanaPod}:/tmp/jaeger.json" -c grafana

kubectl exec -n monitoring $grafanaPod -c grafana -- `
  wget -O- --post-file=/tmp/loki.json `
  --header="Content-Type: application/json" `
  --header="Authorization: Basic YWRtaW46YWRtaW4xMjM=" `
  http://localhost:3000/api/datasources

kubectl exec -n monitoring $grafanaPod -c grafana -- `
  wget -O- --post-file=/tmp/jaeger.json `
  --header="Content-Type: application/json" `
  --header="Authorization: Basic YWRtaW46YWRtaW4xMjM=" `
  http://localhost:3000/api/datasources

# ---------------------------------------------------------------
# 6. Готово
# ---------------------------------------------------------------
Write-Host ""
Write-Host "=== Observability stack installed! ===" -ForegroundColor Green
Write-Host ""
Write-Host "Access Grafana:" -ForegroundColor Cyan
Write-Host "  kubectl port-forward -n monitoring svc/prometheus-grafana 3000:80"
Write-Host "  http://localhost:3000  (admin / admin123)"
Write-Host ""
Write-Host "Datasources: Prometheus, Alertmanager, Loki, Jaeger"
