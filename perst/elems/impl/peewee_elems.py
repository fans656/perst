import json
import contextlib
from typing import Iterable

try:
    import peewee
except ImportError:
    pass

from perst.elems.elems import Elems


class PeeweeElems(Elems):

    def init(self):
        if isinstance(self._source, peewee.ModelBase):
            self.model = _make_model_by_peewee_model(self._source)
        elif callable(self._source):
            self.model = _make_model_by_enterable(self._source)
        else:
            raise RuntimeError(f'unsupported source {self._source}')

    def add(self, elem: dict) -> bool:
        with self.model() as model:
            try:
                model.create(id=elem[self._id_key], data=json.dumps(elem))
            except:
                return False
        return True

    def update_by_elem(self, elem):
        with self.model() as M:
            return M.update({
                M.data: json.dumps(elem),
            }).where(M.id == elem[self._id_key]).execute() > 0

    def remove_by_id(self, elem_id):
        with self.model() as M:
            return M.delete().where(M.id == elem_id).execute() > 0

    def get(self, elem_id):
        with self.model() as M:
            query = M.select(M.data).where(M.id == elem_id).limit(1)
            return next((json.loads(d.data) for d in query), None)

    def __len__(self):
        with self.model() as M:
            return M.select().count()

    def __iter__(self):
        # TODO: chunked for performance
        with self.model() as M:
            return (json.loads(d.data) for d in M.select())


def _make_model_by_peewee_model(peewee_model):
    @contextlib.contextmanager
    def model():
        yield peewee_model
    return model


def _make_model_by_enterable(enterable):
    """
    Example: stome sqlite backend is using following

        @contextlib.contextmanager
        def tables(self, *names):
            with database_lock:
                yield operator.itemgetter(*names)(self.models)

    then it can do:

        PeeweeBackend(self.tables('Storage'))

    and PeeweeBackend can use it equivalent to:

        with self.tables('Storage') as Storage:
            ...
    """
    @contextlib.contextmanager
    def model():
        with enterable() as _model:
            yield _model
    return model
