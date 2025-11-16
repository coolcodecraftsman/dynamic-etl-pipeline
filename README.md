# 📌 Dynamic ETL Pipeline for Unstructured Data
### _Auraverse Hackathon 2025 – Problem Statement 1_

A fully automated **Dynamic ETL Platform** capable of:

- Accepting **unstructured data** (JSON, CSV, TXT key-value)
- **Inferring schemas automatically**
- **Extracting fragments** from large files
- **Tracking schema changes over time**
- Storing data across **MongoDB + PostgreSQL**
- Providing a **visual dashboard** to explore schema evolution & ingested files

This system is built using:
- **FastAPI**
- **MongoDB (for raw fragments)**
- **PostgreSQL (for metadata + schema history)**
- **Streamlit dashboard**
- **Docker Compose**

---

## 🚀 Features

### ✅ 1. Automatic Schema Inference
Supports:
- JSON arrays / objects  
- CSV files  
- Key-value text  
- Auto-detects:
  - Field names
  - Data types
  - Optional fields
  - Nested structures

---

### ✅ 2. Fragment-Based Processing
The system splits files into fragments (chunks):

| Type | Stored In | Purpose |
|------|-----------|----------|
| JSON Fragments | MongoDB | Fast lookup of unstructured JSON blocks |
| CSV Blocks | MongoDB | Row-wise operations |
| Key-Value Blobs | MongoDB | Flexible text processing |

Metadata for each fragment is stored in **PostgreSQL**.

---

### ✅ 3. Schema Registry & Versioning
Each upload generates:

- A new schema snapshot  
- Schema diff vs previous version  
- A version number (v1 → v2 → v3…)  

Dashboard visualizes:
- Added fields  
- Removed fields  
- Modified types  

---

### ✅ 4. Streamlit Dashboard
Shows:

- 📄 Uploaded files  
- 🧩 Extracted fragments  
- 🧬 Schema evolution timeline  
- 🔍 Schema diff viewer  
- 🩺 Backend health status  

---

## 🧱 Technology Stack

### 🟦 Backend (FastAPI)
- FastAPI (REST API)
- SQLAlchemy (ORM for PostgreSQL)
- Pydantic (Validation)
- Motor (Async MongoDB client)
- Uvicorn (ASGI server)

### 🟩 Database Layer
| Purpose | Technology |
|---------|------------|
| Unstructured fragment storage | MongoDB |
| Metadata + schema versions | PostgreSQL |

### 🟪 Frontend (Dashboard)
- Streamlit (UI)
- Requests (API Integration)
- Pandas (Table rendering)

### 🐳 Deployment & DevOps
- Docker
- Docker Compose
- Multi-container networking
- Volume persistence for databases

### 🧰 Utilities / Tools
- Python 3.11
- VSCode (recommended)
- Git & GitHub for version control