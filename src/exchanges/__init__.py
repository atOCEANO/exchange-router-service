import pkgutil
import importlib
import os
import inspect
import logging
from .base import BaseExchange

logger = logging.getLogger(__name__)

EXCHANGE_REGISTRY = {}


def load_exchanges():
    package_dir = os.path.dirname(__file__)
    for _, module_name, is_pkg in pkgutil.iter_modules([package_dir]):
        if is_pkg:
            try:
                module = importlib.import_module(f".{module_name}", package=__package__)
                for name, obj in inspect.getmembers(module):
                    if (inspect.isclass(obj) and issubclass(obj, BaseExchange) and obj is not BaseExchange):
                        adapter = obj()
                        EXCHANGE_REGISTRY[adapter.name] = adapter
                        logger.info(f"Loaded adapter: {adapter.name}")
            except Exception as e:
                logger.error(f"Failed to load adapter '{module_name}': {e}")


def get_adapter(name: str) -> BaseExchange:
    return EXCHANGE_REGISTRY.get(name)