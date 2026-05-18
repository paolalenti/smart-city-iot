# Инструкция по развёртыванию IoT Platform

## Требования
- Windows 10/11
- Docker Desktop (запущен)
- PowerShell от администратора

---

## 1. Установка инструментов

```powershell
# Minikube
winget install Kubernetes.minikube

# Helm
winget install Helm.Helm
```

Установи Cilium CLI:
```powershell
$CILIUM_CLI_VERSION = "v0.18.3"
$url = "https://github.com/cilium/cilium-cli/releases/download/$CILIUM_CLI_VERSION/cilium-windows-amd64.zip"
Invoke-WebRequest -Uri $url -OutFile cilium.zip
Expand-Archive cilium.zip -DestinationPath "$env:USERPROFILE\cilium-cli"
[Environment]::SetEnvironmentVariable("PATH", $env:PATH + ";$env:USERPROFILE\cilium-cli", "User")
```

> После установки закрой и открой терминал заново.

---

## 2. Запуск кластера с Cilium CNI

Запускаем minikube с отключённым CNI по умолчанию — вместо него будет Cilium:

```powershell
minikube start --driver=docker --cpus=4 --memory=6g `
  --network-plugin=cni `
  --cni=false
```

Добавь вторую воркер-ноду:
```powershell
minikube node add
```

Проверь что обе ноды запущены:
```powershell
kubectl get nodes
```

Ожидаемый вывод:
```
NAME           STATUS     ROLES           AGE   VERSION
minikube       NotReady   control-plane   1m    v1.x.x
minikube-m02   NotReady   <none>          30s   v1.x.x
```

> Статус `NotReady` — это нормально, CNI ещё не установлен.

### Установка Cilium

```powershell
helm repo add cilium https://helm.cilium.io/
helm repo update

helm install cilium cilium/cilium `
  --namespace kube-system `
  --set image.pullPolicy=IfNotPresent `
  --set ipam.mode=kubernetes `
  --set operator.replicas=1
```

Проверь статус:
```powershell
cilium status --wait
```

Ожидаемый вывод:
```
Cilium:             OK
Operator:           OK
Envoy DaemonSet:    OK
Cluster Pods:       X/X managed by Cilium
```

---

## 3. Включи Ingress и metrics-server

```powershell
minikube addons enable ingress
minikube addons enable metrics-server
```

Подожди пока поднимется контроллер:
```powershell
kubectl get pods -n ingress-nginx -w
```

Жди статуса `1/1 Running` у `ingress-nginx-controller`.

Переключи ingress-nginx на тип LoadBalancer чтобы работал доступ по домену без проброски портов:
```powershell
kubectl patch svc ingress-nginx-controller -n ingress-nginx --type=merge --patch '{\"spec\":{\"type\":\"LoadBalancer\"}}'
```

---

## 4. Настройка hosts

Получи IP кластера:
```powershell
kubectl get ingress -n iot-helm
```

Добавь в `C:\Windows\System32\drivers\etc\hosts` (от администратора):
```
127.0.0.1    iot-helm.local
```

> IP может отличаться — используй значение из колонки ADDRESS команды выше.

---

## 5. Kubernetes Dashboard

Скачай образы вручную:
```powershell
docker pull kubernetesui/dashboard:v2.7.0
docker pull kubernetesui/metrics-scraper:v1.0.8
minikube image load kubernetesui/dashboard:v2.7.0
minikube image load kubernetesui/metrics-scraper:v1.0.8
```

Установи Dashboard:
```powershell
kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.7.0/aio/deploy/recommended.yaml
```

Исправь политику pull:
```powershell
kubectl patch deployment kubernetes-dashboard -n kubernetes-dashboard `
  -p '{"spec":{"template":{"spec":{"containers":[{"name":"kubernetes-dashboard","imagePullPolicy":"Never"}]}}}}'

kubectl patch deployment dashboard-metrics-scraper -n kubernetes-dashboard `
  -p '{"spec":{"template":{"spec":{"containers":[{"name":"dashboard-metrics-scraper","imagePullPolicy":"Never"}]}}}}'
```

Примени манифест для admin-пользователя (файл `dashboard-admin.yaml` в корне проекта):
```powershell
kubectl apply -f dashboard-admin.yaml
```

Запусти proxy (держи это окно открытым):
```powershell
kubectl proxy
```

Получи токен для входа:
```powershell
kubectl -n kubernetes-dashboard create token admin-user --duration=24h
```

Открой в браузере:
```
http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/
```

Выбери **Token** и вставь полученный токен.

---

## 6. Сборка образов приложения

```powershell
docker build -t iot/api-gateway:latest ./api_gateway
docker build -t iot/device-manager:latest ./device_manager
docker build -t iot/telemetry:latest ./telemetry
docker build -t iot/historical:latest ./hist
docker build -t iot/alert-engine:latest ./alert_engine
docker build -t iot/automation-service:latest ./automation_service
docker build -t iot/notification-service:latest ./notification_service
```

Загрузи образы в minikube (нужно загрузить на все ноды — minikube делает это автоматически):
```powershell
minikube image load iot/api-gateway:latest
minikube image load iot/device-manager:latest
minikube image load iot/telemetry:latest
minikube image load iot/historical:latest
minikube image load iot/alert-engine:latest
minikube image load iot/automation-service:latest
minikube image load iot/notification-service:latest
```

---

## 7. Загрузка инфраструктурных образов

```powershell
docker pull apache/kafka:3.9.0
minikube image load apache/kafka:3.9.0

docker pull influxdb:2.7-alpine
minikube image load influxdb:2.7-alpine
```

---

## 8. TLS сертификат

```powershell
pip install cryptography

python -c "
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import datetime

key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'iot-helm.local')])
cert = (x509.CertificateBuilder()
    .subject_name(subject).issuer_name(issuer)
    .public_key(key.public_key())
    .serial_number(x509.random_serial_number())
    .not_valid_before(datetime.datetime.utcnow())
    .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=365))
    .add_extension(x509.SubjectAlternativeName([x509.DNSName('iot-helm.local')]), critical=False)
    .sign(key, hashes.SHA256()))

open('iot.key','wb').write(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))
open('iot.crt','wb').write(cert.public_bytes(serialization.Encoding.PEM))
print('Done: iot.key and iot.crt created')
"
```

---

## 9. Vault (управление секретами)

### 9.1 Установка Vault

```powershell
helm repo add hashicorp https://helm.releases.hashicorp.com
helm repo update

docker pull hashicorp/vault:1.21.2
minikube image load hashicorp/vault:1.21.2

helm install vault hashicorp/vault `
  --namespace vault `
  --create-namespace `
  --set server.dataStorage.enabled=false `
  --set server.dev.enabled=true `
  --set server.image.pullPolicy=Never
```

Проверь что поды запустились:
```powershell
kubectl get pods -n vault -w
```

Жди `1/1 Running` у `vault-0` и `vault-agent-injector-*`.

### 9.2 Инициализация Vault

В dev-режиме Vault инициализируется автоматически с root token = `root`. Проверь:

```powershell
kubectl exec -n vault vault-0 -- vault login root
```

> Если нужна production-инициализация: `kubectl exec -n vault vault-0 -- vault operator init -key-shares=1 -key-threshold=1 -format=json` — сохрани вывод с `unseal_keys_b64` и `root_token`.

### 9.3 Настройка Vault

```powershell
# Включи Kubernetes auth
kubectl exec -n vault vault-0 -- env VAULT_TOKEN=root vault auth enable kubernetes

# Настрой Kubernetes auth
kubectl exec -n vault vault-0 -- env VAULT_TOKEN=root vault write auth/kubernetes/config `
  kubernetes_host="https://kubernetes.default.svc:443"

# Включи KV secrets engine
kubectl exec -n vault vault-0 -- env VAULT_TOKEN=root vault secrets enable -path=iot-helm kv-v2
```

Создай файл политики `policy.hcl`:
```powershell
@"
path "iot-helm/data/app" {
  capabilities = ["read"]
}
"@ | Set-Content policy.hcl
```

Примени политику:
```powershell
kubectl cp policy.hcl vault/vault-0:/tmp/policy.hcl
kubectl exec -n vault vault-0 -- env VAULT_TOKEN=root vault policy write iot-helm-policy /tmp/policy.hcl
```

Создай роль:
```powershell
kubectl exec -n vault vault-0 -- env VAULT_TOKEN=root vault write auth/kubernetes/role/iot-helm-role `
  bound_service_account_names="default,vault-auth" `
  bound_service_account_namespaces="iot-helm" `
  policies="iot-helm-policy" `
  ttl="72h"
```

### 9.4 Добавь секреты приложения в Vault

```powershell
kubectl exec -n vault vault-0 -- env VAULT_TOKEN=root vault kv put iot-helm/app `
  POSTGRES_USER="iot_user" `
  POSTGRES_PASSWORD="changeme" `
  POSTGRES_DB="iot_db" `
  POSTGRES_DB_TELEMETRY="iot_telemetry" `
  INFLUXDB_URL="http://influxdb:8086" `
  INFLUXDB_TOKEN="my-super-secret-token" `
  INFLUXDB_ORG="iot-org" `
  INFLUXDB_BUCKET="telemetry" `
  INFLUXDB_ADMIN_PASSWORD="adminpassword" `
  BOT_TOKEN="<твой токен>" `
  CHAT_ID="<твой chat id>"
```

Проверь:
```powershell
kubectl exec -n vault vault-0 -- env VAULT_TOKEN=root vault kv get iot-helm/app
```

---

## 10. External Secrets Operator

```powershell
helm install external-secrets oci://ghcr.io/external-secrets/charts/external-secrets `
  --namespace external-secrets `
  --create-namespace
```

Проверь:
```powershell
kubectl get pods -n external-secrets -w
```

Жди `1/1 Running` у всех подов.

---

## 11. Деплой приложения через Helm

```powershell
cd iot-chart
helm dependency update
kubectl create namespace iot-helm
```

Создай TLS Secret (из корня проекта где лежат iot.key и iot.crt):
```powershell
kubectl create secret tls iot-tls --key iot.key --cert iot.crt -n iot-helm
```

Установи чарт:
```powershell
helm install iot . --namespace iot-helm
```

Проверь статус:
```powershell
kubectl get pods -n iot-helm -w
```

Жди пока все поды станут `Running`. Проверь секреты:
```powershell
kubectl get secretstore -n iot-helm
kubectl get externalsecret -n iot-helm
```

Оба должны быть `Ready: True`.

---

## 12. Открытие доступа

В отдельном окне PowerShell от администратора запусти tunnel (держи открытым):
```powershell
minikube tunnel
```

Открой в браузере:
```
https://iot-helm.local/docs
```

Браузер покажет предупреждение о сертификате — нажми **Дополнительно** → **Перейти на сайт**.

---

## 13. Настройка Kafka топиков

Зайди в под Kafka:
```powershell
kubectl exec -it kafka-0 -n iot-helm -- /bin/bash
```

Внутри пода:
```bash
# Просмотр топиков
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Создание топиков
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic alerts --partitions 1 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic telemetry --partitions 1 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic devices --partitions 1 --replication-factor 1

# Просмотр записей в топике
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic alerts --from-beginning
```

---

## 14. Автоскейлинг (HPA)

HPA автоматически масштабирует поды между нодами при росте нагрузки.

Настрой HPA для микросервисов:
```powershell
kubectl set resources deployment api-gateway --requests=cpu=100m -n iot-helm
kubectl set resources deployment device-manager --requests=cpu=100m -n iot-helm
kubectl set resources deployment telemetry-ingestor --requests=cpu=100m -n iot-helm

kubectl autoscale deployment api-gateway --cpu-percent=50 --min=1 --max=3 -n iot-helm
kubectl autoscale deployment device-manager --cpu-percent=50 --min=1 --max=3 -n iot-helm
kubectl autoscale deployment telemetry-ingestor --cpu-percent=50 --min=1 --max=3 -n iot-helm
```

Проверь что HPA работает:
```powershell
kubectl get hpa -n iot-helm
```

Ожидаемый вывод:
```
NAME                 TARGETS       MINPODS   MAXPODS   REPLICAS
api-gateway          cpu: 3%/50%   1         3         1
device-manager       cpu: 4%/50%   1         3         1
telemetry-ingestor   cpu: 5%/50%   1         3         1
```

Посмотри распределение подов по нодам:
```powershell
kubectl get pods -n iot-helm -o wide
```

---

## Команды для отладки

```powershell
# Статус всех подов
kubectl get pods -n iot-helm

# Статус релиза
helm status iot -n iot-helm

# Логи сервиса
kubectl logs deployment/<имя> -n iot-helm

# Описание пода (причина ошибки)
kubectl describe pod <имя-пода> -n iot-helm

# Статус секретов
kubectl get secretstore -n iot-helm
kubectl get externalsecret -n iot-helm

# Проверить что секреты дошли
kubectl get secret iot-secrets -n iot-helm -o yaml

# Статус Cilium
cilium status

# Статус HPA
kubectl get hpa -n iot-helm
```

---

## Возможные проблемы

| Проблема | Решение |
|---|---|
| `ImagePullBackOff` | Загрузи образ через `docker pull` + `minikube image load` |
| `Pending` у подов | Проверь PVC: `kubectl get pvc -n iot-helm` |
| `InvalidProviderConfig` у SecretStore | Проверь что Vault запущен и роль настроена правильно |
| Сайт не открывается | Убедись что `minikube tunnel` запущен от администратора |
| `Permission denied` у Kafka/InfluxDB | Проверь что в манифесте есть `initContainer` с `chown` |
| `<unknown>` в HPA | Задай `resources.requests.cpu` для деплоймента |
| Vault sealed после перезапуска | В dev-режиме не нужен unseal. В prod: `vault operator unseal <UNSEAL_KEY>` |
