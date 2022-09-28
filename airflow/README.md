# Airflow

- https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

```{bash}
sudo docker compose build --no-cache
sudo docker compose up airflow-init
sudo docker compose up -d 
```

```{bash}
docker compose down --volumes --rmi all
```
