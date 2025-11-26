# Spark

## Introduction

The aim of the project is to stream spanning events for spark to consume them and optimize its query. The streaming data can either arrive as a mono-signal or as a bi-signal. 

## Streaming Events

The events were pre-generated into csv's using `stream.py`. It creates `output_data` directory with sub-directories for combinations of mono-signal,bi-signal for both call and tower signals. The code was modified from the given repository [SpanStreamGenerator](https://github.com/bda-lab/SpanStreamGenerator)

### To run

```bash
python stream.py throughput iterations max_call_duration
```

## Queries

The queries that were analyzed were:

`Query 2`: Return tower connections that overlap with a call session but are not fully contained within.
`Query 10`: Calculate time gap between consecutive calls of same user.

## To run scripts

The repository contains both Python and Scala examples. Below are step-by-step instructions to run them. Commands are written for PowerShell where applicable; replace `spark-submit` with the full path to `SPARK_HOME\bin\spark-submit` if it's not on your PATH.

- **Python (data generator and PySpark jobs):**

	- Generate streaming CSV data (mono/bi signals):

		```powershell
		python .\stream.py <throughput> <iterations> <max_call_duration>
		# e.g.
		python .\stream.py 100 1000 300
		```

	- Run the PySpark streaming jobs

		If you have `pyspark` available (or `spark-submit` on PATH) you can run the example Python streaming job directly with `spark-submit`:

		```powershell
		spark-submit .\spark_mono_2.py <files_per_trigger> "<watermark_delay>"
		# e.g.
		spark-submit .\spark_mono_2.py 1 "10 minutes"
		```

		If `spark-submit` is not on your PATH, use the Spark installation path:

		```powershell
		& "$env:SPARK_HOME\bin\spark-submit" .\spark_mono_2.py 1 "10 minutes"
		```

	- Notes:
		- The Python streaming app reads CSVs from `./output_data/mono_signal` and `./output_data/tower_signal` by default.
		- Adjust `<files_per_trigger>` and `<watermark_delay>` to control ingestion and watermark behaviour.

- **Scala (compile + run with spark-submit):**

	- The jar has been provided in `spark_scala.jar`. To recompile:

		```bash
            ./compile_scala.sh
        ```

	- Run with `spark-submit --class`:

		After building `spark_scala.jar` you can run a chosen main object with `spark-submit --class`.

		```pbash
		spark-submit --class spark_bi_2 spark_scala.jar
		spark-submit --class spark_bi_10 spark_scala.jar
		spark-submit --class spark_mono_10 spark_scala.jar <timeout_millis>
		```

	- Notes:
		- `compile_scala.sh` expects `SPARK_HOME` to be set and `scalac`/`jar` available.
.




