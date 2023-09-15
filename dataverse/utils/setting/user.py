
"""
Interface for user setting
"""

import os
import json
from pathlib import Path


class UserSetting:
    """
    Proxy for user setting CRUD, synchronized with the user_setting.json file

    To manage user API keys, passwords, or other sensitive information,
    you can use directly store the information in the user_setting.json file
    or use this class to store the information as proxy. 

    Also, this class is a singleton class, so you can use it anywhere in the code

    caveat:
        - this is just a storage and does not include any logic more than CRUD
        - anything more than CRUD is a responsibility of outside of this class

    what does it means resposibility of outside of this class:
        When user wants to save API key and if does not exist
        asking user to input the API key with stdin might be a good idea.
        But this is not the responsibility of this class.

        This class will only return API key exists or not and base on that,
        outside of this class will ask user to input the API key
        or raise error or whatever it needs to do.
    """
    # Singleton
    _initialized = False

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(UserSetting, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        # when the class is initialized, this is called everytime
        # regardless of the singleton. So adding the flag to check
        if self._initialized:
            return

        # FIXME: when env variable manager is ready, get it from env variable
        cache_path = Path(os.path.abspath(__file__)).parents[3]
        os.makedirs(f"{cache_path}/.cache/dataverse/setting", exist_ok=True)
        self.user_setting_path = os.path.join(cache_path, ".cache/dataverse/setting/user_setting.json")

        # load the user setting, if not exist, empty dict will be assigned
        self.user_setting = self.load(self.user_setting_path)
        self._initialized = True

    def reset(self):
        """
        reset the setting
        """
        self.user_setting = None

    def sync_file(self):
        """
        sync (class -> file)
        """
        with open(self.user_setting_path, "w") as f:
            json.dump(self.user_setting, f, indent=4)

    def sync_class(self):
        """
        sync (file -> class)
        """
        # sync the file to make sure the dict is up-to-date
        self.user_setting = self.load(self.user_setting_path)

    def load(self, path):
        """
        Load the user setting file
        """
        # check if user setting file exists
        if not os.path.exists(path):
            return {}

        # read the file
        with open(path, "r") as f:
            json_file = json.load(f)

        return json_file

    def get(self, key):
        """
        """
        self.sync_class()
        if key not in self.user_setting:
            return None
        return self.user_setting[key]

    def set(self, key, value):
        """
        """
        self.user_setting[key] = value
        self.sync_file()

    def delete(self, key):
        """
        """
        if key in self.user_setting:
            self.user_setting.pop(key, None)
            self.sync_file()
        else:
            raise KeyError(f"Key [ {key} ] does not exist")

    def list(self):
        """
        List all settings
        """
        self.sync_class()
        print(self.user_setting)

    def __repr__(self):
        self.sync_class()
        return json.dumps(self.user_setting, indent=4)

    def __str__(self):
        self.sync_class()
        return json.dumps(self.user_setting, indent=4)
