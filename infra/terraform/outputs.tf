# ============================================================================
# Salidas de la infraestructura. `terraform output ...` expone los valores
# para conectarlos al pipeline (archivo .env).
# ============================================================================

output "project_id" {
  description = "ID del proyecto creado en MongoDB Atlas"
  value       = mongodbatlas_project.proyecto.id
}

output "cluster_name" {
  description = "Nombre del cluster administrado"
  value       = mongodbatlas_cluster.cluster.name
}

output "cluster_tier" {
  description = "Tier del cluster"
  value       = mongodbatlas_cluster.cluster.provider_instance_size_name
}

output "cluster_srv_address" {
  description = "Hostname de conexion (SRV del Replica Set)"
  value       = mongodbatlas_cluster.cluster.srv_address
}

output "connection_string_readwrite" {
  description = "Cadena mongodb+srv:// para el pipeline (copiar a .env como MONGODB_URI)"
  sensitive   = true
  value = "mongodb+srv://${mongodbatlas_database_user.lectura_escritura.username}:${var.db_readwrite_password}@${mongodbatlas_cluster.cluster.srv_address}/sample_mflix?retryWrites=true&w=majority"
}

output "connection_string_readonly" {
  description = "Cadena mongodb+srv:// para herramientas de BI (solo lectura)"
  sensitive   = true
  value = "mongodb+srv://${mongodbatlas_database_user.lectura_solo.username}:${var.db_readonly_password}@${mongodbatlas_cluster.cluster.srv_address}/sample_mflix?retryWrites=true&w=majority"
}

output "ip_access_comment" {
  description = "Comentario del acceso de red configurado"
  value       = mongodbatlas_project_ip_access_list.desarrollo.comment
}