Run Command
----------------
/usr/hdp/2.6.5.0-292/spark2/bin/spark-submit --class com.assignment.scala1.Top10Subscriber --master yarn --deploy-mode cluster --num-executors 4 --executor-memory 4G --driver-memory 4G /data/arpan/arpan_assignment-1.0-SNAPSHOT-jar-with-dependencies.jar
/usr/hdp/2.6.5.0-292/spark2/bin/spark-submit --class com.assignment.scala1.Top5ContentType --master yarn --deploy-mode cluster --num-executors 4 --executor-memory 4G --driver-memory 4G /data/arpan/arpan_assignment-1.0-SNAPSHOT-jar-with-dependencies.jar
/usr/hdp/2.6.5.0-292/spark2/bin/spark-submit --class com.assignment.scala1.TonnageHitsPerDomain --master yarn --deploy-mode cluster --num-executors 4 --executor-memory 4G --driver-memory 4G /data/arpan/arpan_assignment-1.0-SNAPSHOT-jar-with-dependencies.jar
/usr/hdp/2.6.5.0-292/spark2/bin/spark-submit --class com.assignment.scala1.TonnagePerDomainPerContent --master yarn --deploy-mode cluster --num-executors 4 --executor-memory 4G --driver-memory 4G /data/arpan/arpan_assignment-1.0-SNAPSHOT-jar-with-dependencies.jar
/usr/hdp/2.6.5.0-292/spark2/bin/spark-submit --class com.assignment.scala1.TonnagePerGgsn --master yarn --deploy-mode cluster --num-executors 4 --executor-memory 4G --driver-memory 4G /data/arpan/arpan_assignment-1.0-SNAPSHOT-jar-with-dependencies.jar


Database Name
-------------------
arpan_test
