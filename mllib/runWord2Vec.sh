#sbt assembly

#scp target/scala-2.11/demy-machine-learning-library-assembly-1.0.jar sparkrunner:~

ssh sparkrunner 'echo "/space/hadoop/spark_home/bin/spark-submit --class \"demy.mllib.text.Corpus\" --master yarn --executor-cores 3 --driver-cores 3 --num-executors 1 --deploy-mode cluster --name \"Corpus 2 Vectors\" --driver-memory 22g --executor-memory 23g --conf \"spark.driver.maxResultSize=5g\" --conf \"spark.driver.extraJavaOptions=-XX:+UseConcMarkSweepGC\" --conf \"spark.driver.memoryOverhead=1g\" demy-machine-learning-library-assembly-1.0.jar hdfs:///data/text/french hdfs:///data/semantic/common-input/word2vecReduced"  > run.sh; chmod 700 run.sh'
