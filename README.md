<div align="center">

# Dataverse
The Universe of Data. 
All about Data, Data Science, and Data Engineering.

[Tutorials]() • [Contribution Guide]()  • [Contact](mailto:dataverse@upstage.ai)  • [Discord](https://discord.gg/7sswRCad)
<br><br>
<div align="left">

## Welcome to Dataverse!
Dataverse is a freely-accessible open-source project that supports your ETL pipeline with Python. We offer a simple, standardized and user-friendly solution for data processing and management, catering to the needs of data scientists, analysts, and developers. Even though you don't know much about Spark, you can use it easily via _dataverse_.


## 🌌 Installation
### 🌠 Option 1: Git clone
1. Clone _Dataverse_ repository
```bash
git clone https://github.com/UpstageAI/dataverse.git
```
2. Install _Dataverse_
```bash
pip install .
```
3. Install Java
```bash
make java
```
4. Pyspark envrionment setup - `SPARK_HOME` and `PYSPARK_PYTHON`

This is not mandatory, but it is recommended to set up `SPARK_HOME` and `PYSPARK_PYTHON` for pyspark.
```bash
make pyspark
```


### 🌠 Option 2: Install via Pypi \*WIP*
> **Currently, pip install is not supported. Please install Dataverse with option 1.**


1. Install Dataverse with Python's pip package manager:
```bash
pip install dataverse
```
2. Install Java
```bash
sudo apt-get update
sudo apt-get install openjdk-11-jdk
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
source ~/.bashrc
```
3. Pyspark envrionment setup - `SPARK_HOME` and `PYSPARK_PYTHON`

This is not mandatory, but it is recommended to set up `SPARK_HOME` and `PYSPARK_PYTHON` for pyspark.
```bash
pyspark_location=$(pip show pyspark | grep Location | cut -d' ' -f2)
echo "export SPARK_HOME=$pyspark_location/pyspark" >> ~/.bashrc
echo "export PYSPARK_PYTHON=python3" >> ~/.bashrc
source ~/.bashrc
```


## 🌌 AWS S3 Support
> This is not mandatory, but if you want to use AWS, this is required

`SPARK_HOME` is required for the following steps. please make sure you have set `SPARK_HOME` before proceeding. If you didn't check the above section.

### 🌠 Check `hadoop-aws` & `aws-java-sdk` version

#### **hadoop-aws**
version must match with **hadoop** version. you can check your hadoop version by running below command. while writing this README.md the hadoop version was `3.3.4` so the example will use `3.3.4` version.

```python
>>> from dataverse.utils.setting import SystemSetting
>>> SystemSetting().get('HADOOP_VERSION')
3.3.4
```

#### **aws-java-sdk**
version must be compatible with **hadoop-aws** version. Check at Maven [Apache Hadoop Amazon Web Services Support » 3.3.4](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/3.3.4) **Compile Dependencies** section. (e.g. hadoop-aws 3.3.4 is compatible with aws-java-sdk-bundle 1.12.592)


### 🌠 Download `hadoop-aws` & `aws-java-sdk`
> download `hadoop-aws` and `aws-java-sdk` jar files to `$SPARK_HOME/jars` directory.

#### using `Makefile`
```python
make aws_s3
```

#### manual setup
```python
hadoop_aws_jar_url="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
aws_java_sdk_jar_url="https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.592/aws-java-sdk-bundle-1.12.592.jar"
wget -P $SPARK_HOME/jars $hadoop_aws_jar_url
wget -P $SPARK_HOME/jars/ $aws_java_sdk_jar_url
```

### 🌠 Set AWS Credentials
> Currently we do not support ENV variables for AWS credentials but this will be supported in the future. Please use `aws configure` command to set your AWS credentials and this will set `~/.aws/credentials` file which is accessible by `boto3`.

```python
aws configure
```
- `aws_access_key_id`
- `aws_secret_access_key`
- `region`
    - e.g. `ap-northeast-2`


#### When you have session token
> When you have temporary security credentials you have to set `session token` too.

```python
aws configure set aws_session_token <your_session_token>
```

### 🌠 Dataverse is ready to use AWS S3!
> now you are ready to use `Dataverse` with AWS! Every other details will be handled by `Dataverse`!

```python
s3a_src_url = "s3a://your-awesome-bucket/your-awesome-data-old.parquet"
s3a_dst_url = "s3a://your-awesome-bucket/your-awesome-data-new.parquet"

data = spark.read.parquet(s3a_src_url)
data = data.filter(data['awesome'] == True)
spark.write.parquet(data, s3a_dst_url)
```

## 🌌 Contributors
(TBD)

## 🌌 Acknowledgements

Dataverse is an open-source project orchestrated by the **Data-Centric LLM Team** at `Upstage`, designed as an ecosystem for LLM data. Launched in December 2023, this initiative stands at the forefront of advancing data handling in the realm of large language models (LLMs).

## 🌌 License
Dataverse is completely freely-accessible open-source and licensed under the MIT license.


## 🌌 Citation
> If you want to cite our 🌌 Dataverse project, feel free to use the following bibtex

```bibtex
@misc{dataverse,
  title = {Dataverse},
  author = {Hyunbyung Park, Sukyung Lee, Chanjun Park, Yungi Kim, Gyoungjin Gim, Changbae Ahn, Jihoo Kim, Seonghoon Yang},
  year = {2023},
  publisher = {GitHub, Upstage AI},
  howpublished = {\url{https://github.com/UpstageAI/dataverse}},
}
```
