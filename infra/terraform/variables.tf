# ============================================================================
# Variables de infraestructura MongoDB Atlas.
#
# Las variables de credencial deben pasarse como variables de entorno
# TF_VAR_* (NUNCA commitear):
#   $env:TF_VAR_mongodbatlas_public_key  = "..."
#   $env:TF_VAR_mongodbatlas_private_key = "..."
#   $env:TF_VAR_mongodbatlas_org_id      = "..."
#   $env:TF_VAR_db_readwrite_password    = "..."
#   $env:TF_VAR_db_readonly_password     = "..."
#
# El resto puede fijarse en terraform.tfvars (vease terraform.tfvars.example).
# ============================================================================

variable "mongodbatlas_public_key" {
  type        = string
  description = "API Public Key del proyecto/organizacion de MongoDB Atlas"
}

variable "mongodbatlas_private_key" {
  type        = string
  description = "API Private Key del proyecto/organizacion de MongoDB Atlas"
  sensitive   = true
}

variable "mongodbatlas_org_id" {
  type        = string
  description = "ID de la organizacion de MongoDB Atlas (Organization Settings)"
}

variable "project_name" {
  type        = string
  default     = "BD-Parcial"
  description = "Nombre del proyecto que agrupa los recursos (seccion 4.1 del documento)"
}

variable "cluster_name" {
  type        = string
  default     = "BD-Parcial-Cluster"
  description = "Nombre del cluster administrado por Terraform"
}

# ------------------------------------------------------------------ Cluster
variable "cluster_tier" {
  type        = string
  default     = "M0"
  description = "Tier de Atlas. M0 = gratuito (academico). M10 = estandar del curso (~$57/mes)."
}

variable "cluster_provider" {
  type        = string
  default     = "AWS"
  description = "Proveedor para tiers dedicados (M10+). Ignorado en M0 (se usa TENANT)."
}

variable "backing_provider" {
  type        = string
  default     = "AWS"
  description = "Proveedor de respaldo para el tier gratuito M0 (TENANT)"
}

variable "cloud_region" {
  type        = string
  default     = "US_EAST_1"
  description = "Region de despliegue (seccion 4.1.1 del documento)"
}

variable "replica_electable_nodes" {
  type        = number
  default     = 3
  description = "Nodos electables de la Replica Set (documento: 1 primario + 2 secundarios)"
}

# ------------------------------------------------------------------- Red
variable "ip_access_cidr_block" {
  type        = string
  default     = "0.0.0.0/0"
  description = "IP Access List (seccion 4.1.2). Produccion: restringir a IPs de la infraestructura"
}

# ------------------------------------------------------------- Base de datos
variable "db_readwrite_username" {
  type        = string
  default     = "jupagaso_db_user"
  description = "Usuario con privilegios readWriteAnyDatabase para el pipeline (seccion 4.1.3)"
}

variable "db_readwrite_password" {
  type        = string
  description = "Password del usuario readWrite del pipeline"
  sensitive   = true
}

variable "db_readonly_username" {
  type        = string
  default     = "analytics_ro"
  description = "Usuario de solo lectura para BI / dashboards (rol read, seccion 3.6.3)"
}

variable "db_readonly_password" {
  type        = string
  description = "Password del usuario de solo lectura"
  sensitive   = true
}