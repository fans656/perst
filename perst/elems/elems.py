import uuid
from typing import Optional


class Elems:
    """Represent a set of elements persisted.

    Each element is a dict, identified by its "id" key (or user specified key, e.g. "name").

    - id key can be otherwise specified (e.g. using "name" instead of "id")

        perst.elems('data.json', id_key='name')

    - id can be type of str/bytes/int/float

        elems.add({'id': 3.14, 'name': 'pi'})

    Element can be get/added/updated/removed from the elements.

        elems.get('1')

        elems.add({'id': '1', 'name': 'foo'})

        elems.update('1', {'name': 'bar'})

        elems.remove('1')

    The data can be persisted using different storage (e.g. json, sqlite, etc).
    See `impl/` for available storages.
    """

    def __init__(
            self,
            source: any,
            *,
            id_key: str = 'id',
            id_type: type = str,
            data_key: str = 'data',
            fields: list[str] = [],
    ):
        self._source = source
        self._id_key = id_key
        self._id_type = id_type
        self._data_key = data_key
        self._fields = fields

        self.init()

        self._id_to_elem = self._load()

    def add(self, elem: dict) -> any:
        """Add an element to the elements.

        Returns:
            element id - if successfully added
            None - if already in the elements
        """
        if self._id_key in elem:
            elem_id = elem[self._id_key]
        elif self._id_type is str:
            elem_id = elem[self._id_key] = uuid.uuid4()
        elif self._id_type is int:
            elem_id = elem[self._id_key] = max(self._id_to_elem.keys() or (0,)) + 1
        else:
            raise RuntimeError('unsupported auto id')

        if elem_id in self._id_to_elem:
            return False
        else:
            self._id_to_elem[elem_id] = elem
            self.dump()
            return True

    def update(self, *args) -> bool:
        """Update existing element in the elements.

        There are multiple ways to do update.

        1. Update by whole element data:

            elems.update({'id': '1', 'name': 'foo', 'age': 35})

        2. Update by ID and partial data:

            elems.update('1', {'name': 'foo'})

        3. Update using a update function:

            @elems.update('1')
            def _(elem):
                elem['name'] = 'foo'

        Returns:
            True - if successfully updated
            False - if the element does not exist
        """
        if len(args) == 1:
            if not isinstance(args[0], dict):
                elem_id = args[0]
                def deco(update_func):
                    elem = self[elem_id]
                    try:
                        update_func(elem)
                    except Exception:
                        pass
                    else:
                        return self.update_by_elem(elem)
                return deco
            else:
                return self.update_by_elem(args[0])
        elif len(args) == 2:
            elem_id, update = args
            if self._id_key in update and update[self._id_key] != elem_id:
                elem = self[elem_id]
                self.remove(elem_id)
                elem.update(update)
                self.add(elem)
                return True
            else:
                elem = self[elem_id]
                elem.update(update)
                return self.update_by_elem(elem)
        else:
            raise TypeError(f'invalid update arguments {args}')

    def remove(self, elem: any) -> bool:
        """Remove an element from the elements.

        There are multiple ways to do removal.

        1. Remove using element ID:

            elems.remove('1')

        2. Remove using the element dict (only "id" key required):

            elems.remove({'id': '1', 'name': 'foo'})

        Params:

            (id: any) - element ID

            (elem: dict) - element data

        Returns:

            True - if successfully removed
            False - if element does not exist
        """
        if isinstance(elem, dict):
            elem_id = elem[self._id_key]
        else:
            elem_id = elem

        return self.remove_by_id(elem_id)

    def remove_by_id(self, elem_id):
        if elem_id in self._id_to_elem:
            del self._id_to_elem[elem_id]
            self.dump()
            return True
        else:
            return False

    def get(self, elem_id: any) -> Optional[any]:
        return self._id_to_elem.get(elem_id)

    def __contains__(self, target: any) -> bool:
        if isinstance(target, dict):
            elem_id = target[self._id_key]
        else:
            elem_id = target
        return self.get(elem_id) is not None

    def __getitem__(self, elem_id: any) -> any:
        elem = self.get(elem_id)
        if elem is None:
            raise KeyError(f'{elem_id} not found')
        return elem

    def __iter__(self):
        yield from self._id_to_elem.values()

    def __len__(self):
        return len(self._id_to_elem)

    def __bool__(self):
        return len(self) > 0

    def init(self):
        pass

    def load(self) -> list[dict]:
        pass

    def update_by_elem(self, elem: dict) -> bool:
        elem_id = elem[self._id_key]

        if elem_id not in self._id_to_elem:
            return False

        self._id_to_elem[elem_id] = elem
        self.dump()
        return True

    def _load(self):
        elems = self.load() or []
        return {elem[self._id_key]: elem for elem in elems}
