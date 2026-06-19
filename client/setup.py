from setuptools import setup, find_packages

setup(
    name="exchange-router-client",
    version="4.0.1",
    description="Client SDK for the Exchange Router Service",
    packages=find_packages(),
    package_data={"exchange_router_client": ["py.typed"]},
    include_package_data=True,
    install_requires=[
        "httpx>=0.24.0",
        "pandas>=2.0.0",
        "websockets>=12.0",
    ],
    python_requires=">=3.9",
)