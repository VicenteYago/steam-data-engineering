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

To add more realism the datasets are placed in a **S3 bucket in AWS**.

![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/steam.jpg)

Since the **reviews** datasets is ~ 40 GB and ~ 500k files a special processing in Spark is performed. 


## Dashboard

![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/dashboard.png)

## Orchestration


## Improvements

## Resources
