terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.31"
    }
  }
  required_version = ">= 1.0"
}

provider "kubernetes" {
  config_path    = "~/.kube/config"
  config_context = "minikube"
}

# ---------------------------------------------------------------
# Namespaces
# ---------------------------------------------------------------

resource "kubernetes_namespace" "iot_helm" {
  metadata {
    name = "iot-helm"
    labels = {
      managed-by = "terraform"
    }
  }
}

resource "kubernetes_namespace" "vault" {
  metadata {
    name = "vault"
    labels = {
      managed-by = "terraform"
    }
  }
}

resource "kubernetes_namespace" "external_secrets" {
  metadata {
    name = "external-secrets"
    labels = {
      managed-by = "terraform"
    }
  }
}

resource "kubernetes_namespace" "monitoring" {
  metadata {
    name = "monitoring"
    labels = {
      managed-by = "terraform"
    }
  }
}

resource "kubernetes_namespace" "argocd" {
  metadata {
    name = "argocd"
    labels = {
      managed-by = "terraform"
    }
  }
}

# ---------------------------------------------------------------
# Kubernetes Dashboard — ServiceAccount + ClusterRoleBinding
# ---------------------------------------------------------------

resource "kubernetes_service_account" "dashboard_admin" {
  metadata {
    name      = "admin-user"
    namespace = "kubernetes-dashboard"
  }
}

resource "kubernetes_cluster_role_binding" "dashboard_admin" {
  metadata {
    name = "admin-user"
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = "cluster-admin"
  }

  subject {
    kind      = "ServiceAccount"
    name      = "admin-user"
    namespace = "kubernetes-dashboard"
  }
}

# ---------------------------------------------------------------
# ArgoCD — ServiceAccount (задел под задание 2.2)
# ---------------------------------------------------------------

resource "kubernetes_service_account" "argocd" {
  metadata {
    name      = "argocd-manager"
    namespace = kubernetes_namespace.argocd.metadata[0].name
    labels = {
      managed-by = "terraform"
    }
  }
}

resource "kubernetes_cluster_role_binding" "argocd_manager" {
  metadata {
    name = "argocd-manager"
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = "cluster-admin"
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.argocd.metadata[0].name
    namespace = kubernetes_namespace.argocd.metadata[0].name
  }
}

# ---------------------------------------------------------------
# Vault policy secret (базовый секрет для Vault policy)
# ---------------------------------------------------------------

resource "kubernetes_config_map" "vault_policy" {
  metadata {
    name      = "vault-iot-policy"
    namespace = kubernetes_namespace.vault.metadata[0].name
    labels = {
      managed-by = "terraform"
    }
  }

  data = {
    "policy.hcl" = <<-EOT
      path "iot-helm/data/app" {
        capabilities = ["read"]
      }
    EOT
  }
}
