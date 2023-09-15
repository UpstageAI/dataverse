
# Setting
> Setting includes Environment Variables, User Secrets


## User Settings
> API keys, passwords, or other sensitive information of user

### Where does it store?
> Setting will be stored in `cache` with the name of `user_setting.json`.

```python
{CACHE_DIR}/.cache/dataverse/setting/user_setting.json
```
 
### How to modify?
1. You could modify the file directly
2. or can use proxy class `UserSetting`
    - this is synchronized with the `user_setting.json` file

```python
from dataverse.utils.setting import UserSetting
```

### How to use `UserSetting` proxy?

```python
from dataverse.utils.setting import UserSetting

user_setting = UserSetting()

# get the value
user_setting.get('key')

# set the value
user_setting.set('key', 'value')
```