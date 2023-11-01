import os

from setuptools import find_packages, setup

basedir = os.path.abspath(os.path.dirname(__file__))
requirements_path = os.path.join(basedir, "requirements.txt")


def get_requirements():
    """Get package requirements from a requirements file (ex: requirements.txt)."""
    with open(requirements_path, "r") as f:
        return f.read().splitlines()


setup(
    name="dataverse",
    version="0.1.0",
    packages=find_packages(),
    author="hyunbyung park",
    author_email="hyunbyung87@gmail.com",
    description="universe of data",
    include_package_data=True,
    install_requires=get_requirements(),
    entry_points={
        "console_scripts": [
            "dataverse = dataverse.api.cli:main"
        ]
    },
)