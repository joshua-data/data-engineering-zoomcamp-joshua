### Create a Dataproc Cluster

1. Go to CGP Dataproc, then create a cluster.

- Cluster Name: de-zoomcamp-cluster
- Location: asia-northeast3 (same as GCS Bucket's location)
- Cluster Type: Single Node
- Components: Jupyter Notebook and Docker
- Admin Node: N1 (n1-standard-4)

### Submit a job from Dataproc UI

1. Create a folder named `code` in the GCS Bucket.
- from the UI

2. Copy `12-start.py` to GCS Bucket.

```shell
gsutil cp 12-start.py gs://de-zoomcamp-joshua-ny-taxi/code/12-start.py
```

3. Click "Submit a job" from the cluster created.

- Job Type: PySpark
- Main python file: gs://de-zoomcamp-joshua-ny-taxi/code/12-start.py
- Arguments: Add the three things below.

    ```plain
    --green_path=gs://de-zoomcamp-joshua-ny-taxi/pq/green/*/*
    --yellow_path=gs://de-zoomcamp-joshua-ny-taxi/pq/yellow/*/*
    --save_path=gs://de-zoomcamp-joshua-ny-taxi/report/revenue/
    ```

4. Click `Submit` button. After a few minutes, you'll find the `report` directory in your Bucket.

### Submit a job with `gcloud` CLI.

1. Execute the command below from the VM Instance.

```shell
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=asia-northeast3 \
    gs://de-zoomcamp-joshua-ny-taxi/code/12-start.py \
    -- \
        --green_path=gs://de-zoomcamp-joshua-ny-taxi/pq/green/*/* \
        --yellow_path=gs://de-zoomcamp-joshua-ny-taxi/pq/yellow/*/* \
        --save_path=gs://de-zoomcamp-joshua-ny-taxi/report/revenue/
```

- (Optional) When you bump into the Access Error, you'll need to add a permission `Dataproc Administrator` from the IAM.

2. Check out the job result from Dataproc UI!