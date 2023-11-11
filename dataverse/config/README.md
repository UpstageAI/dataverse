# Configuration
> This directory contains configuration files for the Dataverse application


## 🌌 How to use

### 🌠 Load pre-built configuration
> you can load the pre-built configuration from path, or dict, or OmegaConf

```python
from dataverse.config import Config

config = Config.load('path/to/config.yaml')
config = Config.load({
    "spark": {"appname": "README.md example"}
    "etl": [
        {"name": "...", "args": "..."},
        {"name": "...", "args": "..."},
    ]
})
```

### 🌠 Set the empty args with `default` value
> the args you already set will not be changed to default

```python
from dataverse.config import Config

config = Config.load('path/to/config.yaml')
config = Config.set_default(config)
```

### 🌠 Get `Default` configuration
> `default` configuration has no `etl` pre-defined

```python
from dataverse.config import Config

config = Config.default()
```


## 🌌 About Configuration

### 🌠 Why configuration is just `OmegaConf`?
> To make it simple and easy to use. We are not going to inherit some other `base` class to make it complicated. But still `Config` interface is provided as a helper for to load, save, set default, etc.

### 🌠 2 Rules for configuration
1. `One file` rules `ALL`
2. `10 Seconds` to know what is going on

#### `One file` rules `ALL`
One cycle of ETL, Analyzer, etc. which we could call one job, will be controled by one configuration file. We are not going to use multiple configuration files to composite one big configuration file.

#### `10 Seconds` to know what is going on
The reader should be able to know what is going on in the configuration file within 10 seconds. This is to make sure the configuration file is easy and small enough to read and understand.


### 🌠 What open source to choose for configuration?
> **`omegaconf`**

- `OmegaConf`
    - For ease understanding & usage
    - Omegaconf supports yaml, dict, json and even `dataclass` from python.
- `hydra`
    - hydra was also our candidate but to make it simple we are using OmegaConf. 
    - hydra requires multiple configuration files to composite one big configuration file
    - also many people find out using hydra itself took quite a time just to understand
