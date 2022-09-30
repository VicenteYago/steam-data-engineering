# Steam Data Engineering

<p align="center">
<img src="https://user-images.githubusercontent.com/16523144/190527411-9fd2439e-3516-4199-97ef-9fda8fd733b3.png" width="100" height="100">
</p>



## Summary

This project creates a pipeline for processing and analyze the raw data scraped from the videogame digitial distribution platform [**Steam**](). The data is procesed in an **ELT** pipeline with the objective of provide insight about the best selling videogames since 2006. 


## Dataset

The dataset is originary from [**Kaggle**](https://www.kaggle.com/datasets/souyama/steam-dataset), a repository of free datasets for data science. The dataset is composed as a mix of directly scraped records from **Steam** itself and [**Steamspy**](https://steamspy.com/), a website dedicated to collect actual and speculative data about the former platform. All the files are enconded as raw jsons and need and extensive work of transformation in order to make then usable. Additionally all the **reviews** for each game are also retrieved from [**another Kaggle dataset**](https://www.kaggle.com/datasets/souyama/steam-reviews).

## Tools & Technologies
* Cloud - [**Google Cloud Platform**](https://cloud.google.com/)
* Infraestructure as Code - [**Terraform**](https://www.terraform.io/)
* Containerization - [**Docker**](https://www.docker.com/), [**Docker Compose**](https://docs.docker.com/compose/)
* Orchestration - [**Airflow**](https://airflow.apache.org/)
* Transformation - [**Spark**](https://spark.apache.org/)
* Transformation - [**dbt**](https://www.getdbt.com/)
* Data Lake - [**Google Cloud Storage**](https://cloud.google.com/storage)
* Data Warehouse - [**BigQuery**](https://cloud.google.com/bigquery)
* Data Visualization - [**PowerBI**](https://powerbi.microsoft.com/en-au/)
* Language - **Python**

## Architecture

To add more realism the datasets are placed in a **S3 bucket in AWS**. The **trasfer from AWS to GCP** is done trough **Terraform** as part of the initialization tasks, a one-time operation. 

![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/steam.jpg)

Since the **reviews** datasets is ~ 40 GB and ~ 500k files a special processing in **Spark** is performed. 
The **Airflow** server is located in a vitual machine deployed in **Compute Engine GCP** .

## Dashboard

For a more compacted visualization only games with metacritic score are displayed (3.5k out of 51k).

![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/dashboard.png)
 
 A note about **Revenue**: 
 The revenue is calculated as a product of `owners * price`. This is a pretty naive approximation, see [Steamspy guidelines about "Owned" and "Sold" equivalence](https://steamspy.com/about).
 
 
## Orchestration

The **DAG** has to main branches : 

* **Store branch** : selected JSON files are converted into parquet and then loaded as tables in **Bigquery** in parallel.
* **Reviews branch** : JSON files are compacted before the cluster is created, then processed in dataproc as a **pyspark job**. Finally the results are injected into **Bigquery**, where they are merged with other tables using **dbt**. Cluster creation and deletion is fully automated.

Both branches start by retrieving the respective data from the **GCS Buckets**, and they converge in the **dbt** task wich builds the definitive tables in **BigQuery**.

![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/airflow_graph.png)


## Transformation
Transformations are done with Spark (20%) and dbt (80%).

### Spark 

The spark job consists in process all 500k JSON files of reviews in a temporal **Dataproc** cluster and write them in a **Bigquery** table.

### dbt 
All the transformation for the steam store data is done in **dbt** using **SQL** intensively. The main table `steam_games` is built following the [google recommendations](https://cloud.google.com/blog/topics/developers-practitioners/bigquery-explained-working-joins-nested-repeated-data), i.e. is a **denormalized table** featuring nested an repeated structers to avoid the cost of performing joins.

![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/lineage_dbt_1.png)

In order to built the dashboard a custom unnested tables are also built from `steam_games`. 


## Resources
- https://datatalks.club/
