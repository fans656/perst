import contextlib

import pytest


class ElemsForTest:

    def __init__(self, make_elems, elems):
        self._make_elems = make_elems
        self._elems = elems

    def __bool__(self):
        return self._elems.__bool__()

    def __len__(self):
        return self._elems.__len__()

    def __iter__(self):
        return self._elems.__iter__()

    def __getitem__(self, key):
        return self._elems.__getitem__(key)

    def __contains__(self, target):
        return self._elems.__contains__(target)

    def __getattr__(self, key):
        return getattr(self._elems, key)

    def verify(self, func):
        func()

        with self.__using_elems(self._make_elems()):
            func()

    @contextlib.contextmanager
    def __using_elems(self, elems):
        orig_elems = self._elems
        self._elems = elems
        yield
        self._elems = orig_elems


@pytest.fixture(params=[
    'make_json_elems',
    'make_peewee_elems',
])
def make_elems(request):
    """
    Sample usage:

        make_elems('data.sqlite', id_key='name', table='person')

        make_elems.conf(conf, 'data.sqlite', id_key='name', table='person')
    """

    def _make(conf: dict, args: tuple, kwargs: dict) -> ElemsForTest:
        typed_make_elems = request.getfixturevalue(request.param)
        make = lambda: typed_make_elems(conf, args, kwargs)
        elems = make()
        return ElemsForTest(make, elems)

    def _make_elems(*args, **kwargs):
        return _make({}, args, kwargs)

    _make_elems.conf = lambda conf, *args, **kwargs: _make(conf, args, kwargs)

    return _make_elems
