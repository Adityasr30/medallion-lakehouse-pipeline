# Medallion Architecture using Databricks DLT Pipelines

## 1. Objective
Design and implement a scalable Supply Chain Data Model using SAP and IBP raw datasets.  
The solution includes:  
- Metadata-driven ingestion framework
- Medallion architecture (Bronze, Silver, Gold layers) using Delta Live Tables (DLT)
- Business-ready fact and dimension tables
- Power BI dashboard with KPIs derived from the KPI sheet  
The objective is to transform raw SAP data into analytical insights for:  
- Purchase Order monitoring
- Inventory performance tracking
- Demand analysis

## 2. Solution Architecture
High-Level Architecture  

<img width="1026" height="300" alt="image" src="https://github.com/user-attachments/assets/668e9fe7-be7c-49c8-9d89-ae6d42cadbe2" />  

### Architecture Components  
### 2.1. Data Source
- 37 SAP tables (CSV format)
- IBP Demand actual data
- Stored in raw_data/ directory

### 2.2. Databricks
- Metadata-driven ingestion
- Delta Live Tables (DLT)
- 3-layer Medallion architecture

### 2.3. Power BI
- Connected via HTTP path & server hostname
- Data model relationships defined
- KPI-driven dashboards

## 3. Medallion Architecture Implementation
The solution follows a 3-layer Medallion Architecture:  

<img width="442" height="261" alt="image" src="https://github.com/user-attachments/assets/f1b2f6a9-2b5b-4c75-abfb-984ea9e43218" />  

### 3.1. Bronze Layer – Raw Ingestion
**Objective**  
Ingest raw CSV files without transformation.
**Approach**  
- Metadata-driven framework using config_file.json
- Single notebook dynamically loads all tables
- Uses a loop to read table names and source paths
- Creates Delta tables in Bronze schema
**Key Features**  
- Schema inference
- Raw data preservation
- Audit columns (ingestion timestamp)
- No transformations applied  

<img width="364" height="300" alt="image" src="https://github.com/user-attachments/assets/cd626304-fbb2-42d4-abc6-3bc99de4a51a" />  

### 3.2. Silver Layer – Clean & Transform
**Objective**  
Apply transformations based on the Mapping Sheet.  
**Transformations Performed**  
- Column renaming
- Data type casting
- Null handling
- Data Quality (DQ) checks
- Deduplication
- Table joins
- Business rule application  
**Data Quality Checks**  
- Null validations on key columns
- Primary key uniqueness
- Date format validation
- Referential integrity checks  
Silver layer ensures:  
Clean, structured, and validated data ready for modeling.  

<img width="369" height="244" alt="image" src="https://github.com/user-attachments/assets/bf52afa8-0a0e-4f82-89d4-c17ea25bec04" />  

### 3.3. Gold Layer – Business-Ready Model  
**Objective**  
Create analytical data model for reporting.  

**Design Pattern:**  
Star Schema  

**Components:**  

**Fact Tables**  
- Purchase Orders
- Inventory
- Demand

**Dimension Tables**  
- Material
- Vendor
- Plant
- Date
- Customer

**SCD Implementation**  
- SCD Type 1 (Overwrite strategy)
- Used for master data dimensions  

Gold layer provides:  
Optimized and aggregated business-level datasets.  
 
<img width="411" height="390" alt="image" src="https://github.com/user-attachments/assets/6d9fc09f-1ea1-4b61-ba8f-8fcfdda8f158" />  

## 4. Metadata-Driven Framework
**Config File (config_file.json)**  
**Contains:**  
- Table Name
- Source Path
- Target Layer
- Load Type
**Benefits**  
- Reusable ingestion code
- Easy onboarding of new tables
- Reduced manual intervention
- Scalable design  

<img width="940" height="871" alt="image" src="https://github.com/user-attachments/assets/17f60fc7-bb84-483a-95c7-a953edf51d86" />  

## 5. Directory Structure in Databricks  

<img width="599" height="489" alt="image" src="https://github.com/user-attachments/assets/d4fc0e27-87e4-4870-aede-6221c9041eba" />  

## 6. Delta Live Tables (DLT) Pipeline:  
 
<img width="940" height="450" alt="image" src="https://github.com/user-attachments/assets/04ed37d9-8f9c-4775-891b-88b75e12440f" />  

## 7. Data Model  

<img width="940" height="483" alt="image" src="https://github.com/user-attachments/assets/5cfe915a-9442-460b-ba47-c9997e2f5a5a" />  

### 7.1. Fact and Dimension Tables  
 
<img width="771" height="571" alt="image" src="https://github.com/user-attachments/assets/a0233440-1f51-48da-84bd-c69c9fe8f8ce" />  


## 8. Reports Developed  
### 8.1. Purchase Orders Report  

<img width="940" height="532" alt="image" src="https://github.com/user-attachments/assets/7ec5e8e6-4b94-4666-9fa8-a1a3e4390bee" />  

### 8.2. Inventory Overview Report  
 
<img width="940" height="532" alt="image" src="https://github.com/user-attachments/assets/e302cbdb-7bec-4bed-8bd0-77e4db264324" />  

### 8.3. Demand Analysis Report  

<img width="940" height="532" alt="image" src="https://github.com/user-attachments/assets/5378f04a-d3ba-4d53-a3c2-8cccfc153a99" />  

## 9. Conclusion  
This project successfully implements a modern Supply Chain Analytics Platform using:
- Databricks Medallion Architecture
- Metadata-driven ingestion
- Delta Live Tables
- Star Schema modeling
- Power BI dashboards
The solution enables:
- Real-time procurement visibility
- Inventory optimization
- Demand forecasting insights
- Data-driven decision-making
