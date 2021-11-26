import json
import os
from os.path import exists, join

class JSonExporter:
    def __init__(self, json_dir, console=None):
        if os.path.exists(json_dir):
            raise RuntimeError(f"{json_dir} already exists!")
        self.console = console
        self.json_dir = json_dir

    def make_app_json(self, app_name, app_command_data, data_dir):
        """
        Make the json files for a single application
        """
        for c, d in app_command_data.items():
            with open(f'{join(data_dir, app_name)}_{c}.json', 'w') as f:
                json.dump(d.pod(), f, indent=4, sort_keys=True)

    def write_json_files(self, app_command_data, system_command_data):
        """
        Write the per-application and whole-system command data as json files in `json_dir`
        """

        self.console.rule("JSON file creation")

        data_dir = join(self.json_dir, 'data')
        os.makedirs(data_dir)

        # Apps
        for app_name, command_data in app_command_data.items():
            self.make_app_json(app_name, command_data, data_dir)

        # System commands
        for cmd, cfg in system_command_data.items():
            with open(join(self.json_dir, f'{cmd}.json'), 'w') as f:
                json.dump(cfg, f, indent=4, sort_keys=True)

        self.console.log(f"System configuration generated in directory '{self.json_dir}'")
