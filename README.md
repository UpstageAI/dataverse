# Dataverse
> The Universe of Data. All about Data, Data Science, and Data Engineering.

Dataverse, a freely-accessible open-source project on GitHub, offers a simple, standardized and user-friendly solution for data processing and management, catering to the needs of data scientists, analysts, and developers.



## 🌌 Where to start?

### 🌠 ETL Guideline
> recommend starting with ETL guideline. check out below links! It will give you a glimpse of what you can do with `dataverse`.

- [ETL Guideline](https://github.com/UpstageAI/dataverse/tree/main/guideline/etl)
    - ETL_01_how_to_run.ipynb
    - etc.

## 🌌 Installation

### 🌠 Install `dataverse`
`caveat` - we are installing pyspark with `pip` and this does not guarantee standalone pyspark installation. 

```python
pip install dataverse
```

### 🌠 Install Java
> user for pyspark

#### using `Makefile`
```python
make java
```

#### manual installation
if you want to install java manually, you can do so by following the instructions below:

```python
sudo apt-get update
sudo apt-get install openjdk-11-jdk
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
source ~/.bashrc
```

### 🌠 Pyspark Environment Setup - `SPARK_HOME` and `PYSPARK_PYTHON`
> This is not mandatory, but it is recommended to set up `SPARK_HOME` and `PYSPARK_PYTHON` for pyspark.

#### using `Makefile`
```python
make pyspark
```

#### manual setup
```python
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


#### When you have session token
> When you have temporary security credentials you have to set `session token` too.

```python
aws configure set aws_session_token <your_session_token>
```

### 🌠 Dataverse is ready to use AWS S3!
> now you are ready to use `dataverse` with AWS! Every other details will be handled by `dataverse`!

```python
s3a_src_url = "s3a://your-awesome-bucket/your-awesome-data-old.parquet"
s3a_dst_url = "s3a://your-awesome-bucket/your-awesome-data-new.parquet"

data = spark.read.parquet(s3a_src_url)
data = data.filter(data['awesome'] == True)
spark.write.parquet(data, s3a_dst_url)
```


## 🌌 Acknowledgements

Dataverse is an open-source project orchestrated by the **Data-Centric LLM Team** at `Upstage`, designed as an ecosystem for LLM data. Launched in December 2023, this initiative stands at the forefront of advancing data handling in the realm of large language models (LLMs).


## 🌌 Citation
> If you want to cite our 🌌 Dataverse project, feel free to use the following bibtex

```bibtex
@misc{dataverse,
  title = {Dataverse},
  author = {Hyunbyung, Park},
  year = {2023},
  publisher = {GitHub, Upstage AI},
  howpublished = {\url{https://github.com/UpstageAI/dataverse}},
}
```
