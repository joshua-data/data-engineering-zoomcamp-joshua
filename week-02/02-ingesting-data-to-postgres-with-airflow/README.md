# Practice Guide covering:
- `02-ingesting-data-to-postgres-with-airflow`
- `02-local-postgres`

---

1. Download airflow docker-compose.yaml

```shell
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.1/docker-compose.yaml'
```

2. Make dags, logs, and plugins directories

```shell
mkdir -p ./dags ./logs ./plugins
```

3. Write AIRFLOW_UID on .env file

```shell
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
4. Create Dockerfile

```dockerfile
# First-time build can take upto 10 mins.

FROM apache/airflow:2.9.1

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq
# git gcc g++ -qqq

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

WORKDIR $AIRFLOW_HOME

COPY scripts scripts
RUN chmod +x scripts

USER $AIRFLOW_UID
```

5. Create requirements.txt

```plain
apache-airflow-providers-google
pyarrow
```

6. Edit docker-compose.yaml

```yaml
AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
```

7. Build an image from docker-compose.yaml

```shell
docker-compose build
```

8. Initialize Airflow

```shell
docker-compose up airflow-init
```

9. Run docker compose

```shell
docker-compose up -d
```

10. Write DAG and reference python file
    - `data_ingesting_postgres_dag.py`
    - `ingest_data.py`

11. Run another local Postgres.
    - `02-local-postgres`
    - configure networks same as airflow docker-compose.

    ```yaml
    services:
    pg-database:
        image: postgres:13
        environment:
        - POSTGRES_USER=root
        - POSTGRES_PASSWORD=root
        - POSTGRES_DB=ny_taxi
        volumes:
        - "./ny_taxi_postgres_data:/var/lib/postgresql/data:rw"
        ports:
        - "5432:5432"
        networks:
        - airflow

    networks:
    airflow:
        external: true
        name: 02-ingesting-data-to-postgres-with-airflow_default
    ```

12. Run DAG

13. Enter pgcli

```shell
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

14. Check out if the tables have been created following DAG's tasks

```shell
\dt
SELECT COUNT(1) FROM `each table`
```