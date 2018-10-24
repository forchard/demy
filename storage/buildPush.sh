sbt package
cd ../deploy
ansible bgdtz2.oxa -b -m copy -a "src=../storage/target/scala-2.11/demy-storage-layer_2.11-1.0.jar dest={{zeppelin_run}}/custom-libs/"
ansible bgdtz2.oxa -b -m copy -a "src=../core/target/scala-2.11/demy-core_2.11-1.0.jar dest={{zeppelin_run}}/custom-libs/"
cd ../mllib
