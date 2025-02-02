### Modify Script to Connect Spark Result to BigQuery

1. `14-start.py` file is modified from `12-start.py` with below.

- AS-IS

```python
df_result.coalesce(1).write.parquet(save_path, mode='overwrite')
```

- TO-BE

```python
spark.conf.set(
    'temporaryGcsBucket',
    'dataproc-temp-asia-northeast3-204018685474-ksubqm20' # It's a temp GCS Bucket created when you run the cluster job. (See it from GCS Bucket.)
)

...

df_result.write.format('bigquery') \
    .option('table', save_path) \
    .save()
```

2. Run the command below to copy `14-start.py` to Bucket.

```shell
gsutil cp 14-start.py gs://de-zoomcamp-joshua-ny-taxi/code/14-start.py
```

3. Create a dataset named `trips_data_all` in BigQuery.

4. Execute the command below from the VM Instance.

- `jars`: spark-bigquery connector
- `save_path`: bigquery table

```shell
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=asia-northeast3 \
    --jars=gs://spark-lib/bigquery/spark-3.4-bigquery-0.41.0.jar \
    gs://de-zoomcamp-joshua-ny-taxi/code/14-start.py \
    -- \
        --green_path=gs://de-zoomcamp-joshua-ny-taxi/pq/green/*/* \
        --yellow_path=gs://de-zoomcamp-joshua-ny-taxi/pq/yellow/*/* \
        --save_path=trips_data_all.report
```

5. Finally, you can query the table created from BigQuery.

```sql
SELECT  
    *
FROM
    `de-zoomcamp-joshua.trips_data_all.report`
ORDER BY
    1, 2, 3
LIMIT
    1000
```