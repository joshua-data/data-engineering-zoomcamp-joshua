### Install Spark Standalone to a Cluster

1. Run `start-master.sh` to run Spark Master.

```shell
cd
cd spark/spark-3.3.2-bin-hadoop3
./sbin/start-master.sh
```

2. Forward Port 8080 on VS Code.

3. Open http://localhost:8080.
- Only Master is now running. (not the Workers.)

4. Follow thru `12-start.ipynb` to connect Jupyter Notebook to Spark Master.

### Run Workers

1. Run `start-worker.sh` to run Spark Workers to the Spark Master.

```shell
./sbin/start-worker.sh spark://de-zoomcamp.asia-northeast3-c.c.de-zoomcamp-joshua.internal:7077
```

- Then you'll see a new worker is now running.

2. Go back to the remaining part on `12-start.ipynb`.

### Run Python Script.

1. Follow thru `12-start.py`.

2. Run the command below.

```shell
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"

python 12-start.py \
    --green_path=data/pq/green/*/* \
    --yellow_path=data/pq/yellow/*/* \
    --save_path=data/report/revenue/
```

3. Sometimes, we need to make the Spark Master URL dynamic when it comes to handling each pipeline code on Airflow.
- `spark-submit` can be used for this.

```shell
URL="spark://de-zoomcamp.asia-northeast3-c.c.de-zoomcamp-joshua.internal:7077"
spark-submit \
    --master="${URL}" \
    12-start.py \
        --green_path=data/pq/green/*/* \
        --yellow_path=data/pq/yellow/*/* \
        --save_path=data/report/revenue/
```


