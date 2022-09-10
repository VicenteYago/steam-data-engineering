

## Install airflow: 

https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html

Download official docker-compose.yml: 

```{bash}
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.3.4/docker-compose.yaml'
```

```{bash}
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```


```{bash}
docker-compose up airflow-init

docker-compose up -d
```