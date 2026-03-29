"""perst.elems: a set of dict elements"""


def elems(*args, **kwargs):
    """Helper function to construct a set of persisted elements.

    Sample usages:

        # persisted using json
        elems = perst.elems('data.json')


        # persisted using sqlite (through peewee model)
        class Person(peewee.Model):

            class Meta:

                database = database

            id = peewee.TextField(primary_key=True)
            data = peewee.TextField()

        elems = perst.elems(Person)


    See `Elems` for elements interface details.
    """
    source = _get_source(args, kwargs)

    make = None
    if callable(source):
        make = _make_peewee_elems
    else:
        if str(source).endswith('.json'):
            make = _make_json_elems

    if make:
        return make(args, kwargs)
    else:
        raise ValueError(f'unsupported source {source}')


def _make_peewee_elems(args, kwargs):
    from perst.elems.impl.peewee_elems import PeeweeElems
    return PeeweeElems(*args, **kwargs)


def _make_json_elems(args, kwargs):
    from perst.elems.impl.json_elems import JsonElems
    return JsonElems(*args, **kwargs)


def _get_source(args, kwargs):
    if args:
        return args[0]
    else:
        if 'path' in kwargs:
            return kwargs['path']

    raise ValueError('no source given')
