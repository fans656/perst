import contextlib

import pytest
import peewee

import perst


@pytest.fixture
def make_peewee_elems(tmp_path):
    def make(conf, args, kwargs):
        Model = conf.get('model')
        if Model is None:
            attrs = {
                kwargs.get('id_key', 'id'): peewee.TextField(primary_key=True),
                kwargs.get('data_key', 'data'): peewee.TextField(),
            }
            Model = type('Model', (peewee.Model,), attrs)
        bind_model(tmp_path, Model)

        return perst.elems(Model, *args, **kwargs)

    yield make


def bind_model(tmp_path, model):
    database_path = tmp_path / 'data.sqlite'
    database = peewee.SqliteDatabase(database_path)

    tables = [model]
    database.bind(tables)
    database.create_tables(tables)

    return database
