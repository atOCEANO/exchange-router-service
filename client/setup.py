import re
from pathlib import Path

from setuptools import setup, find_packages

version = re.search(
    r'__version__\s*=\s*"([^"]+)"',
    (Path(__file__).parent / "exchange_router_client" / "_version.py").read_text(),
).group(1)

setup(
    name="exchange-router-client",
    version=version,
    description="Client SDK for the Exchange Router Service",
    author="OCEANO",
    url="https://github.com/atOCEANO/exchange-router-service",
    license="MIT",
    packages=find_packages(),
    package_data={"exchange_router_client": ["py.typed"]},
    include_package_data=True,
    install_requires=[
        "httpx>=0.24.0",
        "pandas>=2.0.0",
        "websockets>=12.0",
    ],
    python_requires=">=3.10",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
