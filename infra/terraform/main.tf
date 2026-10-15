# ============================================================================
# Terraform - Infraestructura MongoDB Atlas
# Proyecto: BD Parcial / Arquitectura de Datos (sample_mflix)
#
# Gestiona como codigo (Infrastructure as Code) los recursos cloud que exige el
# documento del curso en la seccion 4.1 "Configuracion del Cluster de Base de
# Datos Distribuida":
#
#   4.1.1 Cluster    -> AWS / us-east-1 / Replica Set              (mongodbatlas_cluster)
#   4.1.2 Network    -> IP Access List 0.0.0.0/0                   (mongodbatlas_project_ip_access_list)
#   4.1.3 Database   -> usuario readWriteAnyDatabase / auth admin  (mongodbatlas_database_user)
#   ---------------- -> Proyecto que agrupa los recursos           (mongodbatlas_project)
#
# TIER / COSTO (relacion con la seccion 3.7 del documento):
#   - M0  (default) : GRATIS. Replica administrada por Atlas. Permite ejecutar
#                     `terraform apply` hoy sin costo y es el tier del cluster
#                     academico actual (Cluster0 free).
#   - M10 (estandar del curso): ~$57 USD/mes base + $0.10/GB adicional.
#                     Replica Set de 3 nodos (1 primario + 2 secundarios).
#                     Para aplicarlo use:  terraform apply -var cluster_tier="M10"
#
# SHARDING (seccion 3.5 del documento):
#   Terraform configura el CLUSTER; shardear las COLECCIONES es una operacion
#   en tiempo de ejecucion sobre el shell de Atlas (o un driver):
#       sh.shardCollection("sample_mflix.<coleccion>", { "_id": "hashed" })
#   El recurso soporta cluster_type = "SHARDED" para tiers M10+ si se desea un
#   cluster particionado horizontal desde la creacion.
# ============================================================================

terraform {
  required_providers {
    mongodbatlas = {
      source  = "mongodb/mongodbatlas"
      version = "~> 1.12"
    }
  }
  required_version = ">= 1.3"
}

# Credenciales via variables de entorno (TF_VAR_*) o terraform.tfvars.
# NUNCA se hardcodean la public/private key en este archivo.
provider "mongodbatlas" {
  public_key  = var.mongodbatlas_public_key
  private_key = var.mongodbatlas_private_key
}

# ---------------------------------------------------------------- 1. PROYECTO
# Agrupa el cluster y los permisos del trabajo academico.
resource "mongodbatlas_project" "proyecto" {
  name   = var.project_name
  org_id = var.mongodbatlas_org_id
}

# --------------------------------------------------------------- 2. CLUSTER
# Replica Set multi-nodo. Si cluster_tier == "M0" (gratis) el proveedor es
# TENANT (Atlas administra los nodos); para M10+ se provisiona AWS dedicado con
# la configuracion de region/nodos definida por variables.
resource "mongodbatlas_cluster" "cluster" {
  project_id                  = mongodbatlas_project.proyecto.id
  name                        = var.cluster_name
  cluster_type                = "REPLICASET"
  provider_name               = var.cluster_tier == "M0" ? "TENANT" : var.cluster_provider
  backing_provider_name       = var.cluster_tier == "M0" ? var.backing_provider : null
  provider_instance_size_name = var.cluster_tier
  provider_region_name        = var.cluster_tier == "M0" ? var.cloud_region : null

  # Bloque sol o solo para clusters dedicados (M10+): define la replica set.
  dynamic "replication_specs" {
    for_each = var.cluster_tier == "M0" ? [] : [1]
    content {
      num_shards = 1
      regions_config {
        region_name     = var.cloud_region
        electable_nodes = var.replica_electable_nodes
        priority        = 7
        read_only_nodes = 0
      }
    }
  }
}

# ----------------------------------------------------- 3. RED (NETWORK ACCESS)
# Seccion 4.1.2 del documento: whitelist abierta para desarrollo academico.
# En produccion se debe restringir a las IPs del Data Lake / servidor de ETL.
resource "mongodbatlas_project_ip_access_list" "desarrollo" {
  project_id = mongodbatlas_project.proyecto.id
  cidr_block = var.ip_access_cidr_block
  comment    = "Acceso de desarrollo academico (seccion 4.1.2 del documento)"
}

# --------------------------------------------- 4. USUARIOS (DATABASE ACCESS)
# Seccion 4.1.3: usuario principal del pipeline con readWriteAnyDatabase
# (autenticacion SCRAM-SHA-256 sobre la base admin, estandar de Atlas).
resource "mongodbatlas_database_user" "lectura_escritura" {
  project_id         = mongodbatlas_project.proyecto.id
  username           = var.db_readwrite_username
  password           = var.db_readwrite_password
  auth_database_name = "admin"

  roles {
    role_name     = "readWriteAnyDatabase"
    database_name = "admin"
  }

  labels {
    key   = "proyecto"
    value = var.project_name
  }
}

# Usuario de solo lectura para herramientas de BI / dashboards (rol "read"
# contemplado en la seccion 3.6.3 del documento).
resource "mongodbatlas_database_user" "lectura_solo" {
  project_id         = mongodbatlas_project.proyecto.id
  username           = var.db_readonly_username
  password           = var.db_readonly_password
  auth_database_name = "admin"

  roles {
    role_name     = "readAnyDatabase"
    database_name = "admin"
  }

  labels {
    key   = "proyecto"
    value = var.project_name
  }
}