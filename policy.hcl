path "iot-helm/data/app" {
  capabilities = ["read"]
}
path "iot-helm/metadata/*" {
  capabilities = ["list"]
}
