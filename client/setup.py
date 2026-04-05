from setuptools import setup, find_packages

setup(
    name="exchange-router-client",
    version="1.0.0",
    description="Client SDK for the Exchange Router Service",
    packages=find_packages(),
    install_requires=[
        "httpx>=0.24.0",
        "pandas>=2.0.0",
        "websockets>=12.0",
    ],
    python_requires=">=3.8",
)