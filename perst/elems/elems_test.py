import contextlib
from pathlib import Path

import pytest

import perst


class Test_usage:

    def test_simple(self, tmp_path):
        # init using a file path
        elems = perst.elems(tmp_path / 'data.json')

        # add an element
        assert elems.add({'id': 1, 'name': 'foo'})

        # get the element
        assert elems.get(1)

        # update the element
        assert elems.update(1, {'name': 'bar'})

        # remove the element
        assert elems.remove(1)


class Test_init_json:
    """Can be persisted using json"""

    def test_json(self, tmp_path):
        fpath = str(tmp_path / 'data.json')

        # init elems by json file path
        elems = perst.elems(fpath)

        elems.add({'id': '1'})
        assert Path(fpath).exists()


class Test_init_peewee:
    """Can be persisted using sqlite (peewee)"""

    def test_model(self, tmp_path):
        """Can use peewee model"""
        model, database_path = self.make_peewee_model(tmp_path)

        elems = perst.elems(model)

        elems.add({'id': '1'})
        assert database_path.exists()

    def test_func_returning_model(self, tmp_path):
        """Can use func returning peewee model"""
        model, database_path = self.make_peewee_model(tmp_path)

        elems = perst.elems(lambda: model)

        elems.add({'id': '1'})
        assert database_path.exists()

    def test_context_manager(self, tmp_path):
        """Can use context manager yielding peewee model"""
        model, database_path = self.make_peewee_model(tmp_path)

        @contextlib.contextmanager
        def get_model():
            yield model

        # init elems by context manager
        elems = perst.elems(get_model)

        elems.add({'id': '1'})
        assert database_path.exists()

    def test_func_returning_context_manager(self, tmp_path):
        """Can use func returning context manager yielding peewee model"""
        model, database_path = self.make_peewee_model(tmp_path)

        @contextlib.contextmanager
        def get_model():
            yield model

        # init elems by func returning context manager
        elems = perst.elems(lambda: get_model())

        elems.add({'id': '1'})
        assert database_path.exists()

    def make_peewee_model(self, tmp_path):
        import peewee

        database_path = tmp_path / 'data.sqlite'
        database = peewee.SqliteDatabase(database_path)

        class Model(peewee.Model):

            id = peewee.TextField(primary_key=True)
            data = peewee.TextField()

        tables = [Model]
        database.bind(tables)
        database.create_tables(tables)

        return Model, database_path


class Test_add:
    """Can add element"""

    def test_add(self, make_elems):
        elems = make_elems()
        assert elems.add({'id': '1', 'name': 'foo'})


class Test_get:
    """Can get element"""

    def test_get(self, make_elems):
        elems = make_elems()
        assert elems.add({'id': '1', 'name': 'foo'})

        @elems.verify
        def _():
            assert elems.get('1') == {'id': '1', 'name': 'foo'}
            assert elems.get('2') is None


class Test_in:

    def test_in(self, make_elems):
        elems = make_elems()
        elems.add({'id': 1})

        @elems.verify
        def _():
            assert 1 in elems
            assert {'id': 1} in elems


class Test_len:
    """Can get number of elements"""

    def test_len(self, make_elems):
        elems = make_elems()
        assert elems.add({'id': '1', 'name': 'foo'})

        @elems.verify
        def _():
            assert len(elems) == 1

        assert elems.add({'id': '2', 'name': 'bar'})

        @elems.verify
        def _():
            assert len(elems) == 2


class Test_update:
    """Can update existing element"""

    def test_update_by_whole_data(self, elems):
        elem = {'id': 1, 'name': 'bar', 'age': 3}

        # update by whole data
        elems.update(elem)

        assert elems[1] == elem

    def test_update_by_partial_data(self, elems):
        # update by id and partial data
        elems.update(1, {'name': 'bar'})

        assert elems[1]['name'] == 'bar'
        assert elems[1]['age'] == 3

    def test_update_replace(self, elems):
        # replace element
        elems.update(1, {'id': 2, 'name': 'bar'})

        assert elems.get(1) is None
        assert elems[2]['name'] == 'bar'
        assert elems[2]['age'] == 3

    def test_update_by_decorator(self, elems):

        # update by func
        @elems.update(1)
        def _(elem):
            elem['name'] = 'bar'

        assert elems[1]['name'] == 'bar'

    @classmethod
    @pytest.fixture()
    def elems(cls, make_elems):
        elems = make_elems(id_type=int)
        elems.add({'id': 1, 'name': 'foo', 'age': 3})
        return elems


class Test_remove:
    """Can remove existing element"""

    def test_remove_by_key(self, elems):
        # remove by id
        elems.remove(1)

        @elems.verify
        def _():
            assert elems.get(1) is None

    def test_remove_by_elem(self, elems):
        # remove by whole data
        elems.remove(elems[1])

        @elems.verify
        def _():
            assert elems.get(1) is None

    @classmethod
    @pytest.fixture()
    def elems(cls, make_elems):
        elems = make_elems()
        elems.add({'id': 1, 'name': 'foo'})
        return elems


def test_same_id_will_not_be_added_again(make_elems):
    elems = make_elems()
    assert elems.add({'id': 1, 'name': 'foo'})
    assert not elems.add({'id': 1, 'name': 'bar'})


def test_iter(make_elems):
    elems = make_elems()
    elems.add({'id': 1})
    elems.add({'id': 2})

    @elems.verify
    def _():
        for elem in elems:
            assert elem


class Test_bool:
    """Can check elements emptiness"""

    def test_empty(self, make_elems):
        elems = make_elems()

        @elems.verify
        def _():
            assert not elems

    def test_non_empty(self, make_elems):
        elems = make_elems()
        assert elems.add({'id': '1'})

        @elems.verify
        def _():
            assert elems


class Test_custom_id_key:
    """Can specify different id key
    """

    def test_name(self, make_elems):
        elems = make_elems(id_key='name')
        assert elems.add({'name': 'foo'})

        @elems.verify
        def _():
            assert elems.get('foo') == {'name': 'foo'}


class Test_id_types:
    """Can use different type for ID"""

    def test_str(self):
        self.verify(str, 'foo')

    #def test_bytes(self, make_elems):
    #    self.verify(bytes, b'\x01\x02\x03')

    def test_int(self, make_elems):
        self.verify(int, 123)

    def test_float(self, make_elems):
        self.verify(float, 1.23)

    def verify(self, id_type, id_value):
        elems = self.make_elems(id_type=id_type)
        elem = {'id': id_value}
        assert elems.add(elem)

        @elems.verify
        def _():
            assert elems.get(id_value) == elem

    @pytest.fixture(autouse=True)
    def setup_class(self, make_elems):
        self.make_elems = make_elems
