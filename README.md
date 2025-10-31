# 🧠 Detección de Fraccionamiento Transaccional – Prueba Técnica NEQUI

Este proyecto aborda el reto de **identificar malas prácticas transaccionales**, en particular el **fraccionamiento de transacciones**, donde un usuario divide una operación grande en varias más pequeñas dentro de un periodo corto de tiempo.  
El objetivo es diseñar un **producto de datos** que permita detectar, analizar y actualizar este tipo de comportamientos de manera sistemática y escalable.

---

## 📂 Estructura general del proyecto

El proyecto está dividido en **dos etapas principales**:

1. **Análisis descriptivo y exploratorio (EDA)**  
   Implementado mediante notebooks de análisis que permiten comprender los datos, identificar patrones y generar las primeras etiquetas de comportamiento fraudulento.

2. **Modelo operativo simulado (ETL + Backend)**  
   Un prototipo funcional desarrollado en `Flask` que simula la actualización diaria de transacciones, detección de fraccionamientos y actualización de tablas agregadas en una base de datos relacional (SQLite en entorno local).

---

## 🧩 Estructura del repositorio

├── app.py
├── data
│ ├── interim
│ │ └── fractioned_transactions.parquet
│ ├── sample_data_0006_part_00.parquet
│ ├── sample_data_0007_part_00.parquet
│ └── utils
│ └── possible_transaction_thresholds.csv
├── database.py
├── instance
│ └── test.db
├── models.py
├── notebooks
│ ├── description.ipynb
│ └── exploration.ipynb
├── README.md
├── requirements.txt
├── start.py
└── utils.py

---

## 🧮 1. Análisis descriptivo (`description.ipynb`)

Este notebook realiza una exploración profunda del conjunto de datos **`sample_data_006_part_00.parquet`**, utilizando la librería **DuckDB**, ideal para trabajar con grandes volúmenes sin cargarlos completamente en memoria.

Los pasos principales incluyen:

- Exploración general (tamaño, tipos de variables, valores nulos, rangos de fechas, valores únicos, outliers).
- Análisis de patrones transaccionales por usuario, cuenta y comercio.
- Identificación de posibles **transacciones fraccionadas**, definidas como transacciones de un mismo usuario al mismo comercio dentro del **mismo día calendario**.
  > Esta decisión evita considerar como fraccionadas transacciones hechas, por ejemplo, a las 6 p.m. y 4 p.m. del día siguiente.

El resultado de este análisis genera un archivo intermedio:

`fractioned_transactions.parquet`

Este archivo contiene:

- `_id`: identificador único de la transacción.
- `label`: identificador común para transacciones que pertenecen a una misma transacción original fraccionada.

---

## 📊 2. Análisis exploratorio (`exploratory.ipynb`)

En esta segunda etapa se profundiza en el análisis de las **transacciones fraccionadas** y su relación con factores clave, tales como:

- Dispersión en los montos de transacción.
- Cantidad de transacciones fraccionadas por comercio y por cuenta.
- Posibles límites de monto establecidos por cada comercio o cuenta.
- Distribución y correlaciones de variables relevantes.

El notebook produce análisis estadísticos agregados por **comercio**, **sucursal** y **número de cuenta**, permitiendo detectar patrones estructurales de comportamiento y relaciones con límites transaccionales.

---

## ⚙️ 3. Backend y modelo operativo simulado (Flask)

Para ilustrar cómo este producto de datos podría integrarse en un entorno operativo, se desarrolló un **modelo de backend** usando el framework **Flask**.

### 🔁 Proceso ETL

El endpoint `/update_transactions` realiza el proceso de **Extracción, Transformación y Carga (ETL)**:

1. Lee nuevas transacciones (por ejemplo desde `sample_data_007_part_00.parquet`).
2. Actualiza las tablas en la base de datos local (SQLite).
3. Identifica nuevas transacciones fraccionadas y actualiza los registros correspondientes.
4. Genera tablas materializadas agregadas por comercio o cuenta, actualizadas **cada 24 horas**.

Las tablas manejadas actualmente son:

- `transactions` – registro completo de transacciones originales.
- `fractioned_transactions` – transacciones identificadas como fraccionadas, vinculadas mediante `_id` (foreign key).

> En un entorno productivo, este esquema podría migrarse fácilmente a PostgreSQL o MySQL, y los archivos procesados a un _data lake_ en la nube (AWS S3, GCP Storage, etc.).

---

## 🧾 4. Configuración y ejecución

### 🧰 Requisitos previos

Instalar las dependencias necesarias:

````bash
pip install -r requirements.txt


Asegúrate de colocar los archivos de datos originales en el directorio data/:

data/
 ├── sample_data_006_part_00.parquet
 └── sample_data_007_part_00.parquet


### 🚀 Configuracion inicial
Para crear la base de datos local y poblarla con datos de ejemplo:

```bash
python start.py
````

Esto generará un archivo SQLite local con las tablas iniciales.

Ejecutar el servidor Flask:

```bash
python app.py
```

El servicio se ejecutará en el puerto 5000 por defecto.
El endpoint principal /update_transactions puede ser accedido vía GET:

```bash
GET http://localhost:5000/update_transactions
```

Este endpoint actualizará las tablas y generará los archivos procesados en:

`data/processed/`

📈 5. Resultados y actualizaciones

Las métricas, distribuciones y correlaciones se recalculan automáticamente con cada actualización.

El sistema permite mantener un flujo continuo de retroalimentación diaria, fortaleciendo la detección de nuevas prácticas de fraccionamiento.

No se incorporan modelos predictivos en esta fase inicial, dado que el enfoque no lo hace necesario, siendo el proceso de identificación basado en reglas claras y definidas. Se agrega valor mediante la automatización y estructuración del proceso.

🧩 6. Pasos para despliegue en produccion

Mejorar el pipeline ETL para fuentes externas (API o mensajería).

Implementar almacenamiento y visualización en dashboards (Power BI, Grafana o Streamlit).

Migración del motor de base de datos a PostgreSQL con tareas automatizadas (Airflow o Prefect).

Contenerización con Docker para despliegue en la nube (AWS, GCP o Azure). Con orquestación con Kubernetes.
