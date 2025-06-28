# 🚀 Data Engineering Challenge

Este proyecto resuelve un reto de **Ingeniería de Datos**, enfocado en el diseño e implementación de una solución de carga, validación, consulta y respaldo de datos relacionados a contrataciones de empleados.

---

## 🛠️ Herramientas y Tecnologías

### 🔄 Procesamiento de Datos
- **Python**: lenguaje principal para el desarrollo de la solución.
- **Pandas**: lectura, transformación y validación de datos desde archivos CSV.
- **UUID**: generación de identificadores únicos para cada registro.

### 🗃️ Almacenamiento en Base de Datos
- **Google BigQuery**: sistema de almacenamiento de datos.
  - Inserción masiva de datos (`INSERT INTO`).
  - Carga directa desde DataFrames.
  - Prevención de duplicados mediante validación previa.

### 🌐 API REST
- **FastAPI**: framework utilizado para construir la API REST.
  - Validación de datos con tipos estrictos.
  - Soporte para carga en lotes (batch).
  - Respuestas claras ante errores (status 400 con detalle).
- **Pydantic**: para la validación de los esquemas de entrada en la API.

### 💾 Backup y Restore
- **Formato AVRO**: respaldo eficiente de registros únicos.
- **fastavro** o **avro-python3**: lectura y escritura de archivos `.avro`.
- Validación de duplicados antes del respaldo.

### 📂 Lectura y Migración Inicial
- **Archivos CSV**: migración de datos históricos.
- Eliminación de duplicados antes de cargar a BigQuery.

### ✅ Buenas Prácticas Aplicadas
- Validación robusta con mensajes específicos.
- Prevención de duplicados tanto en carga como en respaldo.
- Uso de UUID para mantener unicidad.
- Modularización del código.
- Registro de errores en el archivo `logs_validation_errors.txt`.
- Backup seguro: solo se guardan registros nuevos y únicos.

---

## 📁 Estructura del Proyecto



