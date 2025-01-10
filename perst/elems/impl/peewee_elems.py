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
            self.model = _make_model_by_func(self._source)
        else:
            raise RuntimeError(f'unsupported source {self._source}')

    def add(self, elem: dict) -> bool:
        with self.model() as model:
            try:
                fields = {self._id_key: elem[self._id_key]}
                if self._data_key:
                    fields[self._data_key] = json.dumps(elem)
                for field_name in self._fields:
                    fields[field_name] = elem.get(field_name)
                model.create(**fields)
            except:
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
                *self.__get_select_fields(M)
            ).where(
                getattr(M, self._id_key) == elem_id
            ).limit(1)
            return next((self.__get_data_from_model(d) for d in query), None)

    def __len__(self):
        with self.model() as M:
            return M.select().count()

    def __iter__(self):
        # TODO: chunked for performance
        with self.model() as M:
            return (json.loads(getattr(d, self._data_key)) for d in M.select())

    def __get_select_fields(self, Model):
        if self._data_key:
            return (getattr(Model, self._data_key),)
        else:
            return tuple(
                getattr(Model, field_name) for field_name in (self._id_key, *self._fields)
            )

    def __get_data_from_model(self, model):
        if self._data_key:
            return json.loads(getattr(model, self._data_key))
        else:
            return {key: getattr(model, key) for key in (self._id_key, *self._fields)}


def _make_model_by_peewee_model(peewee_model):
    @contextlib.contextmanager
    def model():
        yield peewee_model
    return model


def _make_model_by_func(func):
    """
    Example: stome sqlite backend is using following

        @contextlib.contextmanager
        def tables(self, *names):
            with database_lock:
                yield operator.itemgetter(*names)(self.models)

    then it can do:

        PeeweeElems(lambda: self.tables('Storage'))

    and PeeweeElems can use it equivalent to:

        with self.tables('Storage') as Storage:
            ...
    """
    @contextlib.contextmanager
    def model():
        ret = func()
        if isinstance(ret, peewee.ModelBase):
            yield ret
        elif hasattr(ret, '__enter__'):
            with ret as _model:
                yield _model
        else:
            raise NotImplementedError()
    return model
