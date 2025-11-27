## 1. Requirements

Make sure the following are installed on your system:

- **Apache Flink** (tested with Flink 1.17+)
- **Python 3.8+**
- **PyFlink**
- **Java 11 or 17**

Start the Flink cluster using the command:
```bash
$FLINK_HOME/bin/start-cluster.sh
```

This flink dashboard can be accessed at localhost:8081.

## 2. Execution instructions

Before running any script, make sure to update the local file paths inside the .py files.

Each Flink program accepts a watermark delay (in seconds) as a command-line argument.
```bash
flink run -py <file_name> --watermark <watermark_seconds> 
```

Equivalently, you can also run:
```bash
python <file_name> --watermark <watermark_seconds> 
```


