### Connect to VM Instance.

1. Login to the VM Instance from VS Code using Remote-SSH.

```bash
    ssh de-zoomcamp
```

2. To run PySpark, don't forget to add python to `PYTHONPATH`.

```bash
    export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
    export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
```

- Make sure that the version under ${SPARK_HOME}/python/lib/ matches the filename of py4j or you will encounter `ModuleNotFoundError: No module named 'py4j'` while executing import pyspark.
- Check the spark directory to match the real version to the variable exporting.

### Start Jupyter.

1. Move on to the notebooks directory.

```bash
    cd notebooks/
```

2. Open the port forwarding for jupyter notebook using VS Code.

```bash
    8888
```

3. Then command following from `notebooks/` directory.

```bash
    jupyter notebook
```

4. Copy the showing URL after the command, then paste it to your local browser.

```plain
    http://localhost:8888/tree?token=...
```

5. Open a Remote SSH from VS Code, then open the port forwarding for spark ui.

```bash
    4040
```

6. Go to the url below.

```plain
    http://localhost:4040
```

7. Now, refer to `03-spark-dataframes.ipynb` to write a new notebook there.