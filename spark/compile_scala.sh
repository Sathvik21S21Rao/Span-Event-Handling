rm -rf classes
mkdir -p classes
scalac -classpath "$SPARK_HOME/jars/*" -d classes spark_bi_10.scala
scalac -classpath "$SPARK_HOME/jars/*" -d classes spark_mono_10.scala
scalac -classpath "$SPARK_HOME/jars/*" -d classes spark_bi_2.scala
jar cvf spark_scala.jar -C classes .

