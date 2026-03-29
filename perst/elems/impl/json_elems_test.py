import pytest

import perst


@pytest.fixture
def make_json_elems(tmp_path):

    def make(conf, args, kwargs):
        return perst.elems(tmp_path / 'data.json', *args, **kwargs)

    yield make
