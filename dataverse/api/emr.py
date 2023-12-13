
"""
API to use AWS EMR with spark-submit
"""

from dataverse.etl import ETLPipeline
import argparse


def main(config, verbose=False):
    """Main entry point for the aws emr."""
    etl_pipeline = ETLPipeline()
    spark, data = etl_pipeline.run(config=config, verbose=verbose)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="config file path")
    parser.add_argument("--verbose", action='store_true')
    args = parser.parse_args()
    main(args.config, args.verbose)
