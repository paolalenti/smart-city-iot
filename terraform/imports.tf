# imports.tf
# Импорт ресурсов которые уже существуют в кластере.
# Выполняется один раз: terraform apply после terraform init.

import {
  to = kubernetes_namespace.iot_helm
  id = "iot-helm"
}

import {
  to = kubernetes_namespace.vault
  id = "vault"
}

import {
  to = kubernetes_namespace.external_secrets
  id = "external-secrets"
}

import {
  to = kubernetes_namespace.monitoring
  id = "monitoring"
}

import {
  to = kubernetes_service_account.dashboard_admin
  id = "kubernetes-dashboard/admin-user"
}

import {
  to = kubernetes_cluster_role_binding.dashboard_admin
  id = "admin-user"
}
