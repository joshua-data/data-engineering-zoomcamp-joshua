### Create a GCS Bucket.

1. I've personally created a bucket named `de-zoomcamp-joshua-ny-taxi`.

### Upload Data to GCS Bucket.

1. Locate yourself to `notebooks/data/` in the VM Instance.

```shell
cd notebooks/data/
```

2. Copy the `pq` folder to BSC Bucket.

```shell
gsutil -m cp -r pq/ gs://de-zoomcamp-joshua-ny-taxi/pq
```

2.1. (Optional) In case there's an error, please run the command below.

```shell
gcloud auth login
```

### Put the local `google_credentials.json` file to VM Instance.

```shell
cd ~/.google/credentials/
sftp de-zoomcamp
mkdir .google
cd .google/
mkdir credentials
cd credentials/
put google_credentials.json
exit
```

### Start Jupyter.

1. To run PySpark, don't forget to add python to `PYTHONPATH`.

```bash
    export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
    export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
```

- Make sure that the version under ${SPARK_HOME}/python/lib/ matches the filename of py4j or you will encounter `ModuleNotFoundError: No module named 'py4j'` while executing import pyspark.
- Check the spark directory to match the real version to the variable exporting.

2. Move on to the notebooks directory.

```bash
    cd notebooks/
```

3. Open the port forwarding for jupyter notebook using VS Code.

```bash
    8888
```

4. Then command following from `notebooks/` directory.

```bash
    jupyter notebook
```

5. Copy the showing URL after the command, then paste it to your local browser.

```plain
    http://localhost:8888/tree?token=...
```

6. Open a Remote SSH from VS Code, then open the port forwarding for spark ui.

```bash
    4040
```

7. Go to the url below.

```plain
    http://localhost:4040
```

### Download jar file for Code Leap.

1. Since my Spark version is version 3.3.2, run the command below.

```shell
cd notebooks/
mkdir lib
cd lib/
gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar gcs-connector-hadoop3-2.2.5.jar
```

### Spark Session Setup and Configuration.

1. Go to `11-connecting-to-gcs.ipynb`.
