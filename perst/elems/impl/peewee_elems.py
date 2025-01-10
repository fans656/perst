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
                model.create(**{
                    self._id_key: elem[self._id_key],
                    self._data_key: json.dumps(elem),
                })
            except:
                print(elem)
                import traceback
                traceback.print_exc()
                return False
        return True

    def update_by_elem(self, elem):
        with self.model() as M:
            return M.update({
                getattr(M, self._data_key): json.dumps(elem),
            }).where(M.id == elem[self._id_key]).execute() > 0

    def remove_by_id(self, elem_id):
        with self.model() as M:
            return M.delete().where(M.id == elem_id).execute() > 0

    def get(self, elem_id):
        with self.model() as M:
            query = M.select(
                getattr(M, self._data_key)
            ).where(
                getattr(M, self._id_key) == elem_id
            ).limit(1)
            return next((json.loads(getattr(d, self._data_key)) for d in query), None)

    def __len__(self):
        with self.model() as M:
            return M.select().count()

    def __iter__(self):
        # TODO: chunked for performance
        with self.model() as M:
            return (json.loads(getattr(d, self._data_key)) for d in M.select())


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

        PeeweeElems(self.tables('Storage'))

    and PeeweeElems can use it equivalent to:

        with self.tables('Storage') as Storage:
            ...
    """
    @contextlib.contextmanager
    def model():
        with enterable() as _model:
            yield _model
    return model
