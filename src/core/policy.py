import requests
import asyncio
import uuid

class PolicyPipe():
    def __init__(self):
        # key is a tuple with sink and source
        # value is a list which contains rules to apply to passing data
        self.pipes : dict[tuple[str, str], list[PolicyRule]] = {}
        self.idmap : dict[str]

    def map_pipe(self, sink: str, source: str, rules: list[PolicyRule] = []) -> bool:
        pair = (sink, source)

        if pair in self.pipes:
            return False

        self.pipes.update({pair : rules.copy()})

        self.idmap.update({str(uuid.uuid4()) : pair})

    def interact(self, key: str | tuple, data) -> tuple(bool, str):
        if type(key) is str:
            pair = self.idmap.get(key, None)
            if pair is None:
                return False, ""
            elem = self.pipes.get(pair, None)
        elif type(key) is tuple:
            elem = self.pipes.get(key, None)

        if elem is None:
            return False, ""

        pass
