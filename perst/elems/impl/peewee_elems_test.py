import json
import contextlib

import pytest
import peewee

import perst

from .peewee_elems import PeeweeElems


@pytest.fixture
def make_peewee_elems(tmp_path):
    def make(conf, args, kwargs):
        Model = conf.get('model')
        if Model is None:
            attrs = {
                kwargs.get('id_key', 'id'): get_id_field(kwargs.get('id_type')),
                kwargs.get('data_key', 'data'): peewee.TextField(),
            }
            Model = type('Model', (peewee.Model,), attrs)
        bind_model(tmp_path, Model)

        return perst.elems(Model, *args, **kwargs)

    yield make


def get_id_field(id_type):
    if id_type in (None, str):
        Field = peewee.TextField
    elif id_type is int:
        Field = peewee.IntegerField
    elif id_type is float:
        Field = peewee.FloatField
    else:
        raise NotImplementedError()
    return Field(primary_key=True)


def bind_model(tmp_path, model):
    database_path = tmp_path / 'data.sqlite'
    database = peewee.SqliteDatabase(database_path)

    tables = [model]
    database.bind(tables)
    database.create_tables(tables)

    return database


class Test_custom_data_key:
    """Can specify different data key

    sqlite implementation use "data" column to store the element dict,
    this column name can be specified otherwise (e.g. "meta") to accommodate existing database.
    """

    def test_data_key(self, make_elems):
        elems = make_elems(data_key='meta')
        assert elems.add({'id': '1', 'name': 'foo'})

        @elems.verify
        def _():
            assert elems.get('1') == {'id': '1', 'name': 'foo'}

            if isinstance(elems._elems, PeeweeElems):
                with elems.model() as Model:
                    model = Model.get_or_none('1')
                    assert model.id == '1'
                    assert json.loads(model.meta)['name'] == 'foo'


class Test_custom_database_fields:
    """Can specify extra fields each corresponding to data attribute.

    For example, "name" / "age" fields for {"id": 1, "name": "foo", "age": 35}.
    """

    def test_default(self, make_elems):
        class Model(peewee.Model):

            id = peewee.TextField(primary_key=True)
            name = peewee.TextField(index=True)
            age = peewee.IntegerField(index=True)
            data = peewee.TextField()

        elems = make_elems.conf({'model': Model}, fields=['name', 'age'])
        elems.add({'id': '1', 'name': 'foo', 'age': 35})

        @elems.verify
        def _():
            assert elems.get('1') == {'id': '1', 'name': 'foo', 'age': 35}

            if isinstance(elems._elems, PeeweeElems):
                with elems.model() as Model:
                    model = Model.get_or_none('1')
                    assert model.name == 'foo'
                    assert model.age == 35


class Test_no_data_key:
    """Can specify no data key (so all fields corresponding to database columns).
    """

    def test_default(self, make_elems):
        class Model(peewee.Model):

            id = peewee.TextField(primary_key=True)
            name = peewee.TextField(index=True)
            age = peewee.IntegerField(index=True)

        elems = make_elems.conf({'model': Model}, data_key=None, fields=['name', 'age'])
        elems.add({'id': '1', 'name': 'foo', 'age': 35})

        @elems.verify
        def _():
            assert elems.get('1') == {'id': '1', 'name': 'foo', 'age': 35}

            if isinstance(elems._elems, PeeweeElems):
                with elems.model() as Model:
                    model = Model.get_or_none('1')
                    assert model.name == 'foo'
                    assert model.age == 35


class Test_autoid:

    def test_default(self, make_elems):
        elems = make_elems(id_type=int)
        assert elems.add({'name': 'foo'})

        @elems.verify
        def _():
            _elems = list(elems)
            assert len(_elems) == 1

            _elem = _elems[0]
            assert _elem == {'id': 1, 'name': 'foo'}

        assert elems.add({'name': 'foo'})

        @elems.verify
        def _():
            _elems = list(elems)
            assert len(_elems) == 2

            _elem = _elems[1]
            assert _elem == {'id': 2, 'name': 'foo'}
