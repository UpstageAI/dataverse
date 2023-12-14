
"""
API to use AWS EMR with spark-submit
"""

import os
import argparse
import importlib

from dataverse.etl import ETLPipeline

def import_dynamic_etls():
    """
    Import dynamic etls which was created by user.
    """
    dynamic_etl_path = "/home/hadoop/dataverse/dynamic_etl"
    try:
        files = os.listdir(dynamic_etl_path)
    except FileNotFoundError:
        return
    except Exception as e:
        raise e

    # Filter out non-Python files
    files = [f for f in files if f.endswith('.py')]

    # Dynamically import all Python files in the directory
    for file in files:
        file_path = os.path.join(dynamic_etl_path, file)

        # Remove .py at the end
        module_name = file[:-3]

        spec = importlib.util.spec_from_file_location(module_name, file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)


def main(config, verbose=False):
    """Main entry point for the aws emr."""
    etl_pipeline = ETLPipeline()
    import_dynamic_etls()
    spark, data = etl_pipeline.run(config=config, verbose=verbose)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="config file path")
    parser.add_argument("--verbose", action='store_true')
    args = parser.parse_args()
    main(args.config, args.verbose)
