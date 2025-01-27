### Connect to VM Instance.

1. Create a VM Instance in Google Cloud.

2. Create a SSH Key from Metadata and save it to the following path.

```bash
    `~./.ssh/gcp`
```

3. Create a `config` file in home directory.

```plain
    Host de-zoomcamp
        HostName {External IP of VM Instance}
        User joshua
        IdentityFile /Users/joshuakim/.ssh/gcp
```

4. Login to the VM Instance typing below from the local machine.

```bash
    `ssh de-zoomcamp`
```

### Install Java.

1. make a directory named `spark` and download Java@11.0.2 into it.

```bash
    mkdir spark
    cd spark/
    wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
```

2. Unpack it.

```bash
    tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz
```

3. Remove the archive.

```bash
    rm openjdk-11.0.2_linux-x64_bin.tar.gz
```

4. Define `JAVA_HOME` and add it to `PATH`, so that Spark can reference to it later.

```bash
    export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
    export PATH="${JAVA_HOME}/bin:${PATH}"
```

5. Check that it works.

```bash
    which java
    java --version
```

### Install Spark.

1. Download Spark@3.3.2 into the same `spark` directory.

```bash
    wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
```

2. Unpack it.

```bash
    tar xzfv spark-3.3.2-bin-hadoop3.tgz
```

3. Remove the archive.

```bash
    rm spark-3.3.2-bin-hadoop3.tgz
```

4. Define `SPARK_HOME` and add it to `PATH`.

```bash
    export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"
    export PATH="${SPARK_HOME}/bin:${PATH}"
```

5. Execute `spark-shell` and run the following to test Spark.

```bash
    spark-shell
```

```scala
    val data = 1 to 10000
    val distData = sc.parallelize(data)
    distData.filter(_ < 10).collect()
```

6. Simply enter `:quit` to quit from Spark.

```bash
    :quit
```

### Put all the PATHs to bashrc file.

1. Enter the variables at the bottom of lines in `.bashrc`.

```bashrc
    export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
    export PATH="${JAVA_HOME}/bin:${PATH}"
    export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"
    export PATH="${SPARK_HOME}/bin:${PATH}"
```

2. Re-evaluate the variables.

```bash
    source .bashrc
```

3. Logout from the remote machine and Re-login to apply those variables.

```bash
    logout
    ssh de-zoomcamp
    which java
    which pyspark
```

### Install Python. (if you haven't done so.)

1. Install Acaconda.

```bash
    wget https://repo.anaconda.com/archive/Anaconda3-2024.02-1-Linux-x86_64.sh
    bash Anaconda3-2024.02-1-Linux-x86_64.sh
    rm Anaconda3-2024.02-1-Linux-x86_64.sh
```

2. Connect python3 to python with the symbolic link.

```bash
    sudo ln -sf $(which python3) /usr/bin/python
```

3. Add Acaconda PATH to .bashrc.

```bash
    export PATH="$HOME/anaconda3/bin:$PATH"
    logout
    ssh de-zoomcamp
```

### Start Jupyter.

1. Make a directory named notebooks.

```bash
    mkdir notebooks
    cd notebooks/
```

2. Open a Remote SSH from VS Code, then open the port forwarding for jupyter notebook.

```bash
    8888
```

3. To run PySpark, we first need to add python to `PYTHONPATH`.

```bash
    export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
    export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
```

- Make sure that the version under ${SPARK_HOME}/python/lib/ matches the filename of py4j or you will encounter `ModuleNotFoundError: No module named 'py4j'` while executing import pyspark.
- Check the spark directory to match the real version to the variable exporting.

4. Then command following from `notebooks/` directory.

```bash
    jupyter notebook
```

5. Copy the showing URL after the command, then paste it to your local browser.

```plain
    http://localhost:8888/tree?token=...
```

6. Now, refer to `pyspark.ipynb` to write a new notebook there.

### Spark UI

1. Open a Remote SSH from VS Code, then open the port forwarding for spark ui.

```bash
    4040
```

2. Go to the url below.

```plain
    http://localhost:4040
```