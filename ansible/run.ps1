# run.ps1
# Запуск Ansible Role для Kafka через Docker
# Запускать из корня проекта

$kubeconfig = "$env:USERPROFILE\.kube\config"
$projectRoot = (Get-Location).Path

Write-Host "=== Running Ansible Kafka Role via Docker ===" -ForegroundColor Cyan

# Шаг 1 — установить коллекцию kubernetes.core
docker run --rm `
  -v "${projectRoot}/ansible:/work" `
  alpine/ansible `
  sh -c "apk add --quiet py3-kubernetes && ansible-galaxy collection install -r /work/requirements.yml"

# Получаем IP minikube
$minikubeIp = minikube ip

# Создаём kubeconfig с Linux-путями и правильным IP
$kubeconfigContent = Get-Content "$env:USERPROFILE\.kube\config" -Raw
$kubeconfigContent = $kubeconfigContent -replace [regex]::Escape("C:\Users\Kirill\.minikube"), "/root/.minikube"
$kubeconfigContent = $kubeconfigContent -replace "\\", "/"
$kubeconfigContent = $kubeconfigContent -replace "127\.0\.0\.1:\d+", "${minikubeIp}:8443"
$kubeconfigContent | Set-Content "$projectRoot\ansible\kubeconfig-linux" -NoNewline

# Шаг 2 — запустить playbook
$minikubeDir = "$env:USERPROFILE\.minikube"

docker run --rm `
  -v "${projectRoot}/ansible:/work" `
  -v "${minikubeDir}:/root/.minikube:ro" `
  --network host `
  alpine/ansible `
  sh -c "apk add --quiet py3-kubernetes && KUBECONFIG=/work/kubeconfig-linux ansible-playbook /work/playbook.yml -v"

Write-Host ""
Write-Host "=== Done! ===" -ForegroundColor Green
