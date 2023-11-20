
.PHONY: pyspark java 

pyspark:
	echo "export SPARK_HOME=$(shell pip show pyspark | grep Location | awk '{print $$2 "/pyspark"}')" >> ~/.bashrc
	echo "export PYSPARK_PYTHON=python3" >> ~/.bashrc

# setting java environment
java:
	sudo apt-get update
	sudo apt-get install openjdk-11-jdk
	echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
