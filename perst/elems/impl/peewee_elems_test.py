import contextlib

import pytest
import peewee

import perst


@pytest.fixture
def make_peewee_elems(tmp_path):

    database_path = tmp_path / 'data.sqlite'
    database = peewee.SqliteDatabase(database_path)

    class Person(peewee.Model):

        id = peewee.TextField(primary_key=True)
        data = peewee.TextField(null=True)

    @contextlib.contextmanager
    def get_model():
        tables = [Person]
        database.bind(tables)
        database.create_tables(tables)
        yield Person

    def make(conf, args, kwargs):
        return perst.elems(get_model, *args, **kwargs)

    yield make
