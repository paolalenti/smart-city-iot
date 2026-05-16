output "namespaces" {
  description = "Managed namespaces"
  value = [
    kubernetes_namespace.iot_helm.metadata[0].name,
    kubernetes_namespace.vault.metadata[0].name,
    kubernetes_namespace.external_secrets.metadata[0].name,
    kubernetes_namespace.monitoring.metadata[0].name,
    kubernetes_namespace.argocd.metadata[0].name,
  ]
}

output "argocd_service_account" {
  description = "ArgoCD manager service account"
  value       = kubernetes_service_account.argocd.metadata[0].name
}
