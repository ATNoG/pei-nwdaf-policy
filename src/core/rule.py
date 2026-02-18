from typing import Callable
from inspect import signature

import os

class PolicyRule():
    def __init__(self, action : Callable = None, metadata : list = []):
        self.action : Callable | None = action

        # we may want to store some sort of relevant information for diagnosis,
        # along with some extra functionality (take this as a control over the
        # control of interaction with data).action
        #
        # so this metadata list might come useful
        self.metadata : list = []

    def _validate_action_sig() -> bool:
        """Returns true whether the embedded action complies with the defined structure"""
        sig = signature(self.action)
        if len(sig.parameters) != 1:
            return False

        # There might be more validation in the future...

        return True

    def __call__(self, data):
        return self.action()
