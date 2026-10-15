# Terraform - MongoDB Atlas (Infrastructure as Code)

Provisión declarativa de los recursos cloud que el documento del curso define en
el **Capítulo 4.1 "Configuración del Cluster de Base de Datos Distribuida"**, con
los estándares de la sección 3.5 (Replicación/Sharding) y 3.7 (Costos cloud).

```
infra/terraform/
├── main.tf               # Recursos: proyecto, cluster, red, usuarios
├── variables.tf          # Parámetros (tier, región, nombres, roles)
├── outputs.tf            # Cadenas de conexión para el .env del pipeline
├── terraform.tfvars.example
└── README.md
```

## Recursos gestionados

| Recurso terraform        | Componente Atlas (doc)                 |
|--------------------------|----------------------------------------|
| `mongodbatlas_project`   | Proyecto que agrupa el cluster         |
| `mongodbatlas_cluster`   | Replica Set AWS `us-east-1`, 3 nodos   |
| `mongodbatlas_project_ip_access_list` | Network Access `0.0.0.0/0`    |
| `mongodbatlas_database_user` (x2) | readWriteAnyDatabase + read    |

Tier por defecto **M0 (gratis)**: `terraform apply` funciona hoy sin costo y
equivale al cluster free de desarrollo. El estándar del curso es **M10**
(~$57 USD/mes, ver sección 3.7 del documento); se activa con
`-var cluster_tier="M10"` (provisiona AWS dedicado con Replica Set de 3 nodos).

## 1. Requisitos previos

1. **Instalar Terraform** (Windows, PowerShell como administrador):
   ```powershell
   choco install terraform -y      # o winget install HashiCorp.Terraform
   terraform -version
   ```
2. **Crear API Key en Atlas**: Organization Access Manager → API Keys → Create
   API Key (permisos **Organization Owner** para crear proyectos) → copiar
   Public Key y Private Key.
3. **Obtener el Organization ID**: Organization Settings → General Settings.

## 2. Configurar credenciales (sin secretos en el repo)

```powershell
$env:TF_VAR_mongodbatlas_public_key  = "<tu_public_key>"
$env:TF_VAR_mongodbatlas_private_key = "<tu_private_key>"
$env:TF_VAR_mongodbatlas_org_id      = "<tu_org_id>"
$env:TF_VAR_db_readwrite_password    = "<password_del_pipeline>"
$env:TF_VAR_db_readonly_password     = "<password_del_dashboards>"
```

Los demás parámetros van en `terraform.tfvars` (copie el `.example`):

```powershell
cd infra/terraform
Copy-Item terraform.tfvars.example terraform.tfvars
# edite tier/región/puerto si lo desea
```

## 3. Ciclo de vida

```powershell
terraform init        # descarga el proveedor mongodb/mongodbatlas
terraform validate    # valida HCL (sin conexión a Atlas)
terraform plan        # muestra lo que va a crear/actualizar/eliminar
terraform apply       # aplica; preguntará confirmación (o -auto-approve)
```

Al terminar, obtenga las cadenas de conexión:

```powershell
terraform output connection_string_readwrite
terraform output -raw connection_string_readwrite   # para usarla como variable
```

Escriba el valor en el `.env` del pipeline:

```
MONGODB_URI=mongodb+srv://jupagaso_db_user:<PASSWORD>@<SRV>/sample_mflix?retryWrites=true&w=majority
MONGODB_DATABASE=sample_mflix
```

## 4. Gestionar el cluster existente (import) en lugar de crear otro

Si ya existe el cluster académico (Cluster0) y solo se quiere **adoptarlo** con
Terraform para administrarlo como código, importe cada recurso al estado:

```powershell
# 1. Crear solo las credenciales de conexión primero
terraform init
# 2. Importación (necesita el project_id de Atlas: Project Settings -> General)
terraform import mongodbatlas_project.proyecto                   <project_id>
terraform import mongodbatlas_cluster.cluster                     <project_id>-Cluster0
terraform import mongodbatlas_project_ip_access_list.desarrollo   <project_id>-0.0.0.0/0
terraform import mongodbatlas_database_user.lectura_escritura     <project_id>-<usuario>
terraform import mongodbatlas_database_user.lectura_solo          <project_id>-<usuario>
terraform plan    # debe mostrar "No changes"; así el código pasa a gobernar lo existente
```

## 5. Sharding (sección 3.5 del documento)

Terraform provisina el **cluster**; el sharding de **colecciones** se ejecuta en
runtime (shell/consola de Atlas):

```javascript
use sample_mflix
sh.shardCollection("sample_mflix.interacciones_analiticas", { "fecha": 1 })
```

Para un cluster particionado desde el origen, cambie
`cluster_type = "SHARDED"` en `main.tf` (requiere tier M10+).

## 6. Destrucción y costos

```powershell
terraform destroy    # elimina proyecto, cluster, red y usuarios creados
```

Advertencia de costos (sección 3.7 del documento): M0 es gratis; **M10 = ~$57
USD/mes + $0.10/GB adicional** (≈ $63 USD/mes a 50 GB). No deje un cluster M10
activo sin necesidad; use `terraform destroy` al terminar la demo.

## 7. Sustentación (mapeo con el documento)

- **3.5 Particionamiento/Replicación**: Replica Set de 3 nodos (2 para `M0`/gestionado, 3 definibles en
  `replica_electable_nodes`) y el bloque de sharding documentado.
- **3.6 Seguridad**: usuarios SCRAM (roles `readWrite`/`read`, auth `admin`),
  cifrado TLS por defecto con `mongodb+srv://`, secretos por variables.
- **3.7 Costos**: tier configurable M0/M10 y nota de costo mensual.
- **4.1 Configuración del cluster**: los 4 recursos de este `main.tf`
  reproducen punto a punto: proveedor AWS, región `us-east-1`, Replica Set,
  Network Access `0.0.0.0/0` y usuario con `readWriteAnyDatabase`.
- El documento ocupa las capturas de pantalla (`[Insertar captura...]`); puede
  complementarlas con el `terraform plan` y `terraform output` como evidencia
  de que la infraestructura se provisiona de forma reproducible.