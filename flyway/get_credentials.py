from udacore.config_reader import load_config
import argparse

parser = argparse.ArgumentParser(description="flyway deployment credentials")
parser.add_argument("--f", required=True, help="This is the file path for config location")
args = parser.parse_args()
filepath = args.f
response = load_config(filepath)
with open("./export_vars", "w") as f:
    for key, value in response.items():
        if "SNOWFLAKE" in key.upper():
            f.writelines(f'export {key.upper().replace(".", "_")}={value}\n')
