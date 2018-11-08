sbt package
cd ../deploy
ansible spark_edge -b -m copy -a "src=../storage/target/scala-2.11/demy-storage-layer_2.11-1.0.jar dest={{zeppelin_run}}/custom-libs/"
ansible spark_edge -b -m copy -a "src=../core/target/scala-2.11/demy-core_2.11-1.0.jar dest={{zeppelin_run}}/custom-libs/"
ansible spark_edge -b -m copy -a "src=../mllib/target/scala-2.11/demy-machine-learning-library_2.11-1.0.jar dest={{zeppelin_run}}/custom-libs/"
cd ../mllib
