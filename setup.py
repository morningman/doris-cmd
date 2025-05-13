from setuptools import setup, find_packages
import re

# Extract version from __init__.py
with open('doris_cmd/__init__.py', 'r') as f:
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", f.read(), re.M)
    version = version_match.group(1) if version_match else '0.2.0'

setup(
    name="doris-cmd",
    version=version,
    packages=find_packages(),
    install_requires=[
        "mysql-connector-python>=8.0.0",
        "pymysql>=1.0.2",
        "requests>=2.27.1",
        "tabulate>=0.8.9",
        "prompt-toolkit>=3.0.24",
        "click>=8.0.3",
        "pygments>=2.10.0",
    ],
    extras_require={
        'dev': [
            'pytest>=7.0.0',
            'flake8>=4.0.0',
            'black>=22.0.0',
        ],
    },
    entry_points={
        "console_scripts": [
            "doris-cmd=doris_cmd.main:main",
        ],
    },
    author="morningman",
    author_email="morningman.cmy@gmail.com",
    description="A command line client for Apache Doris with query progress reporting",
    keywords="doris, mysql, cli, progress-tracking, runtime",
    python_requires=">=3.6",
) 
