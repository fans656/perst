import json
from pathlib import Path

from perst.elems.elems import Elems


class JsonElems(Elems):

    def init(self):
        self._fpath = Path(self._source)

    def load(self):
        if self._fpath.exists():
            with self._fpath.open() as f:
                return json.load(f)

    def dump(self):
        with self._fpath.open('w') as f:
            json.dump(list(self), f)
