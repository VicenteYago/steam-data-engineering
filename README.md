# Steam Data Engineering

<p align="center">
<img src="https://user-images.githubusercontent.com/16523144/190527411-9fd2439e-3516-4199-97ef-9fda8fd733b3.png" width="100" height="100">
</p>



## Summary

This project creates a pipeline for processing and analyze the raw data scraped from the videogame digitial distribution platform [**Steam**](). The data is procesed in an **ELT** pipeline with the objective of provide insight about the best selling videogames since 2006. 


## Dataset

The dataset is originary from [**Kaggle**](), a repository of free datasets for data science. The dataset is composed as a mix of directly scraped records from **Steam** itself and [**Steamspy**]() a website dedicated to collect actual and speculative data about the former platform. All the files are enconded as raw jsons and need and extensive work of transformation in order to make then usable. Additionally all the **reviews** for each game are also retrieved from [**another Kaggle dataset**]().

## Tools & Technologies
* Cloud - [**Google Cloud Platform**]()
* Infraestructure as Code - [**Terraform**]()
* Containerization - [**Docker**](), [**Docker Compose**]()
* Orchestration - [**Airflow**]()
* Transformation - [**Spark**]()
* Transformation - [**dbt**]()
* Data Lake - [**Google Cloud Storage**]()
* Data Visualization - [**PowerBI**]()
* Language - [**Python**]()

## Architecture

To add more realism the datasets are placed in a **S3 bucket in AWS**. The **trasfer from AWS to GCP** is done trough **Terraform** as part of the initialization tasks, a one-time operation. 

![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/steam.jpg)

Since the **reviews** datasets is ~ 40 GB and ~ 500k files a special processing in **Spark** is performed. 


## Dashboard

For a more compacted visualization only games with metacritic score are displayed (3.5k out of 51k).

![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/dashboard.png)
 
 A note about **Revenue**: 
 The revenue is calculated as a product of `owners * price`. This is a pretty naive approximation, see [Steamspy guidelines about "Owned" and "Sold" equivalence](https://steamspy.com/about).
 
 
## Orchestration

The **DAG** has to main branches : 

* **Store branch** : selected JSON files are converted into parquet and then loaded as tables in **Bigquery** in parallel
* **Reviews branch** : JSON files are compacted before the cluster is created, then processed in dataproc as a **pyspark job**. Finally the results are injected into **Bigquery**, where they are merged with other tables using **dbt**. Cluster creation and deletion is fully automated.

Both branches start by retrieving the respective data from the **GCS Buckets**, and they converge in the **dbt** task wich builds the definitive tables in **BigQuery**.

![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/airflow_graph.png)


## Transformation

### Spark

### dbt

![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/lineage_dbt_1.png)

## Improvements

## Resources
