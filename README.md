# dataverse
> The Universe of Data. All about data, data science, and data engineering.

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

## 🌌 Where to start?

### 🌠 ETL Guideline
> recommend starting with ETL guideline. check out below links! It will give you a glimpse of what you can do with `dataverse`.

- [ETL Guideline](https://github.com/UpstageAI/dataverse/tree/main/guideline/etl)
    - ETL_01_how_to_run.ipynb
    - etc.

