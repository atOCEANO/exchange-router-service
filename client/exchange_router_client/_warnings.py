import warnings
from typing import List


class RouterDataWarning(UserWarning):
    pass


def emit(messages: List[str], verbose: bool) -> None:
    if not verbose:
        return

    for message in messages:
        warnings.warn(message, RouterDataWarning, stacklevel=3)
