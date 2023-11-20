
.PHONY: aws pyspark java 

aws:
	@test -d $$SPARK_HOME/jars || mkdir -p $$SPARK_HOME/jars
	@test -f $$SPARK_HOME/jars/hadoop-aws-3.3.4.jar || wget -P $$SPARK_HOME/jars/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
	@test -f $$SPARK_HOME/jars/aws-java-sdk-bundle-1.12.592.jar || wget -P $$SPARK_HOME/jars/ https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.592/aws-java-sdk-bundle-1.12.592.jar

pyspark:
	echo "export SPARK_HOME=$(shell pip show pyspark | grep Location | awk '{print $$2 "/pyspark"}')" >> ~/.bashrc
	echo "export PYSPARK_PYTHON=python3" >> ~/.bashrc

# setting java environment
java:
	sudo apt-get update
	sudo apt-get install openjdk-11-jdk
	echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
