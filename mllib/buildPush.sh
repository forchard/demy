cd ../core
sbt package
cd ../storage
sbt package
cd ../mllib
sbt package
scp "../storage/target/scala-2.11/demy-storage-layer_2.11-1.0.jar" "sparkrunner:/space/hadoop/zeppelin_run/custom-libs/"
scp "../core/target/scala-2.11/demy-core_2.11-1.0.jar" "sparkrunner:/space/hadoop/zeppelin_run/custom-libs/"
scp "../mllib/target/scala-2.11/demy-machine-learning-library_2.11-1.0.jar" "sparkrunner:/space/hadoop/zeppelin_run/custom-libs/"
