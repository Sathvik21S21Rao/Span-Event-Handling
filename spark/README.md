# Spark

## To run scripts

The repository contains both Python and Scala examples. Below are step-by-step instructions to run them. Commands are written for PowerShell where applicable; replace `spark-submit` with the full path to `SPARK_HOME\bin\spark-submit` if it's not on your PATH.

- **Python (data generator and PySpark jobs):**

	- Generate streaming CSV data (mono/bi signals) from `../stream.py`

	- Run the PySpark streaming jobs

		If you have `pyspark` available (or `spark-submit` on PATH) you can run the example Python streaming job directly with `spark-submit`:

		```bash
		spark-submit .\spark_mono_2.py "<watermark_delay>" <cores>
		spark-submit .\spark_mono_2.py 1 "10 seconds" 4
		```

		If `spark-submit` is not on your PATH, use the Spark installation path:

		```bash
		$SPARK_HOME\bin\spark-submit .\spark_mono_2.py "10 seconds"
		```

	- Notes:
		- The Python streaming app reads CSVs from `../output_data/mono_signal` and `../output_data/tower_signal` by default.

- **Scala (compile + run with spark-submit):**

	- The jar has been provided in `spark_scala.jar`. To recompile:

		```bash
            ./compile_scala.sh
        ```

	- Run with `spark-submit --class`:

		After building `spark_scala.jar` you can run a chosen main object with `spark-submit --class`.

		```bash
		spark-submit --class spark_bi_2 spark_scala.jar <watermark_millis> <timeout_millis> <cores>
		spark-submit --class spark_bi_10 spark_scala.jar <watermark_millis> <timeout_millis> <cores>
		spark-submit --class spark_mono_10 spark_scala.jar <watermark_millis> <timeout_millis> <cores>
		```

	- Notes:
		- `compile_scala.sh` expects `SPARK_HOME` to be set and `scalac`/`jar` available.
.




