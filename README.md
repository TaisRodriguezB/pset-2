Proyecto PSet 2

Objetivo
Construir una solución end-to-end para ingerir, almacenar y transformar datos de NY Taxi utilizando una arquitectura ELT reproducible.

Arquitectura
El proyecto utiliza:
PostgreSQL → Data warehouse
Mage → Orquestación de pipelines
pgAdmin → Exploración de datos
Docker Compose → Infraestructura reproducible

Flujo de datos 
NY Taxi Data
   ↓
Pipeline RAW (Mage)
   ↓
PostgreSQL (schema raw)
   ↓
Pipeline CLEAN (Mage)
   ↓
PostgreSQL (schema clean - modelo estrella)

Cómo ejecutar el proyecto
1. Clonar el repositorio
git clone https://github.com/TaisRodriguezB/pset-2.git
cd pset-2
2. Levantar el entorno
docker compose up -d
3. Acceder a servicios
Mage: http://localhost:6789
pgAdmin: http://localhost:5050

Configuración
Las credenciales se gestionan mediante variables de entorno en .env.

Base de datos
Se crean automáticamente dos schemas:
raw → datos crudos
clean → datos transformados

