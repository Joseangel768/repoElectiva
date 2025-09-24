# Customer Opinions ETL

Este proyecto implementa un pipeline ETL (Extract, Transform, Load) para cargar datos de opiniones de clientes en una base de datos SQL Server.

## Estructura del repositorio
- sql/create_db.sql: Script para crear la base de datos y tablas.
- src/etl_pipeline.py: Script de Python para ejecutar el ETL.
- docs/: Documentación del proyecto.

## Requisitos
- Python 3.9+
- Paquetes: pandas, sqlalchemy, pyodbc, tqdm, python-dateutil
- SQL Server (con base OpinionesDB creada)

## Ejecución
1. Ejecutar el script SQL en `sql/create_db.sql` en tu servidor de SQL Server.
2. Crear carpeta `csvs` con archivos `products.csv`, `customers.csv`, etc.
3. Configurar credenciales en `etl_pipeline.py` (línea ENGINE_URL).
4. Ejecutar:
   ```bash
   python src/etl_pipeline.py
   ```

## Validación
Ejecutar en SQL Server:
```sql
SELECT COUNT(*) FROM Products;
SELECT COUNT(*) FROM Customers;
```
