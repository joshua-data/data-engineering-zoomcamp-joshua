### Download Taxi Data.

1. Create a Shell Script `download_data.sh`.

2. Give it the permission.

```shell
chmod +x download_data.sh
```

3. Run the following to start downloading.

```shell
./download_data.sh yellow 2024
./download_data.sh green 2024
./download_data.sh yellow 2023
./download_data.sh green 2023
```

4. Run the following to see the savings architecture.

```shell
# Just in case you haven't installed tree yet:
    # sudo apt update
    # sudo apt install tree -y
    # tree --version
tree data
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

8. Now, refer to `04-prepare-taxi-data.ipynb` to write a new notebook there.