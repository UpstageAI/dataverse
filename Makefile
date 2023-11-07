
.PHONY: java

# setting java environment
java:
	sudo apt-get update
	sudo apt-get install openjdk-11-jdk
	echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
