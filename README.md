# sparkdf
Quick Start

cd sparkdf

mvn clean package

spark-submit --master local --class spark.SparkDF --deploy-mode client target/sparkdf-1.0-SNAPSHOT.jar

