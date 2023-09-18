
# Setting
> Setting includes Environment Variables, User Secrets

## System Settings
> The heart of the system. It contains the configuration of the system.

### Naming Convention
- All uppercase
    - e.g. `CACHE_DIR`

### System Setting Policy
- Only memory (not stored in the file)
- Only updated by `Environment Variables`
- Manually updated - check `system.py.SystemSetting.default_setting()`
- No update after the system is initialized
    - If you want to change the setting, you must restart the system.

### How to modify?
- Only by Setting Environment Variables

```bash
# dynamic
DATASET_CACHE_DIR=/path/to/cache/dir python3 main.py

# static
export DATASET_CACHE_DIR=/path/to/cache/dir
python3 main.py
```

### How to use `SystemSetting`
> **This MUST be used internally by the system**. But just in case, you can use it in 3 ways to use it.

```python
from dataverse.utils.setting import SystemSetting

# get the setting
cache_dir = SystemSetting().get('CACHE_DIR')
cache_dir = SystemSetting()['CACHE_DIR']
cache_dir = SystemSetting().CACHE_DIR

# set the setting
SystemSetting().set('CACHE_DIR', '/path/to/cache/dir')
SystemSetting()['CACHE_DIR'] = '/path/to/cache/dir'
SystemSetting().CACHE_DIR = '/path/to/cache/dir'
```


## User Settings
> API keys, passwords, or other sensitive information of user.

### Where does it store?
> Setting will be stored in `CACHE_DIR` set in `SystemSetting` with the name of `user_setting.json`.

```python
from dataverse.utils.setting import SystemSetting

{SystemSetting().CACHE_DIR}/.cache/dataverse/setting/user_setting.json
```

 
### How to modify?
1. You could modify the `user_setting.json` file directly
2. or can use proxy class `UserSetting`
    - this is synchronized with the `user_setting.json` file

```python
from dataverse.utils.setting import UserSetting
```

### How to use `UserSetting` proxy?
> There is 3 ways to use it.


```python
from dataverse.utils.setting import UserSetting

# get the value
github_api = UserSetting().get('GITHUB_API')
github_api = UserSetting()['GITHUB_API']
github_api = UserSetting().GITHUB_API

# set the value
UserSetting().set('GITHUB_API', 'your_github_api_key')
UserSetting()['GITHUB_API'] = 'your_github_api_key'
UserSetting().GITHUB_API = 'your_github_api_key'
```