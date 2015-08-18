# orm/loading.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""private module containing functions used to convert database
rows into object instances and associated state.

the functions here are called primarily by Query, Mapper,
as well as some of the attribute loading strategies.

"""
from __future__ import absolute_import

from .. import util
from . import attributes, exc as orm_exc
from ..sql import util as sql_util
from . import strategy_options

from .util import _none_set, state_str
from .. import exc as sa_exc

_new_runid = util.counter()

from .loading_speedups import _instance_processor  # noqa


def instances(query, cursor, context):
    """Return an ORM result as an iterator."""

    context.runid = _new_runid()

    filtered = query._has_mapper_entities

    single_entity = len(query._entities) == 1 and \
        query._entities[0].supports_single_entity

    if filtered:
        if single_entity:
            filter_fn = id
        else:
            def filter_fn(row):
                return tuple(
                    id(item)
                    if ent.use_id_for_hash
                    else item
                    for ent, item in zip(query._entities, row)
                )

    try:
        (process, labels) = \
            list(zip(*[
                query_entity.row_processor(query,
                                           context, cursor)
                for query_entity in query._entities
            ]))

        if not single_entity:
            keyed_tuple = util.lightweight_named_tuple('result', labels)

        while True:
            context.partials = {}

            if query._yield_per:
                fetch = cursor.fetchmany(query._yield_per)
                if not fetch:
                    break
            else:
                fetch = cursor.fetchall()

            if single_entity:
                proc = process[0]
                rows = [proc(row) for row in fetch]
            else:
                rows = [keyed_tuple([proc(row) for proc in process])
                        for row in fetch]

            if filtered:
                rows = util.unique_list(rows, filter_fn)

            for row in rows:
                yield row

            if not query._yield_per:
                break
    except Exception as err:
        cursor.close()
        util.raise_from_cause(err)


@util.dependencies("sqlalchemy.orm.query")
def merge_result(querylib, query, iterator, load=True):
    """Merge a result into this :class:`.Query` object's Session."""

    session = query.session
    if load:
        # flush current contents if we expect to load data
        session._autoflush()

    autoflush = session.autoflush
    try:
        session.autoflush = False
        single_entity = len(query._entities) == 1
        if single_entity:
            if isinstance(query._entities[0], querylib._MapperEntity):
                result = [session._merge(
                    attributes.instance_state(instance),
                    attributes.instance_dict(instance),
                    load=load, _recursive={})
                    for instance in iterator]
            else:
                result = list(iterator)
        else:
            mapped_entities = [i for i, e in enumerate(query._entities)
                               if isinstance(e, querylib._MapperEntity)]
            result = []
            keys = [ent._label_name for ent in query._entities]
            keyed_tuple = util.lightweight_named_tuple('result', keys)
            for row in iterator:
                newrow = list(row)
                for i in mapped_entities:
                    if newrow[i] is not None:
                        newrow[i] = session._merge(
                            attributes.instance_state(newrow[i]),
                            attributes.instance_dict(newrow[i]),
                            load=load, _recursive={})
                result.append(keyed_tuple(newrow))

        return iter(result)
    finally:
        session.autoflush = autoflush


def get_from_identity(session, key, passive):
    """Look up the given key in the given session's identity map,
    check the object for expired state if found.

    """
    instance = session.identity_map.get(key)
    if instance is not None:

        state = attributes.instance_state(instance)

        # expired - ensure it still exists
        if state.expired:
            if not passive & attributes.SQL_OK:
                # TODO: no coverage here
                return attributes.PASSIVE_NO_RESULT
            elif not passive & attributes.RELATED_OBJECT_OK:
                # this mode is used within a flush and the instance's
                # expired state will be checked soon enough, if necessary
                return instance
            try:
                state._load_expired(state, passive)
            except orm_exc.ObjectDeletedError:
                session._remove_newly_deleted([state])
                return None
        return instance
    else:
        return None


def load_on_ident(query, key,
                  refresh_state=None, lockmode=None,
                  only_load_props=None):
    """Load the given identity key from the database."""

    if key is not None:
        ident = key[1]
    else:
        ident = None

    if refresh_state is None:
        q = query._clone()
        q._get_condition()
    else:
        q = query._clone()

    if ident is not None:
        mapper = query._mapper_zero()

        (_get_clause, _get_params) = mapper._get_clause

        # None present in ident - turn those comparisons
        # into "IS NULL"
        if None in ident:
            nones = set([
                        _get_params[col].key for col, value in
                        zip(mapper.primary_key, ident) if value is None
                        ])
            _get_clause = sql_util.adapt_criterion_to_null(
                _get_clause, nones)

        _get_clause = q._adapt_clause(_get_clause, True, False)
        q._criterion = _get_clause

        params = dict([
            (_get_params[primary_key].key, id_val)
            for id_val, primary_key in zip(ident, mapper.primary_key)
        ])

        q._params = params

    if lockmode is not None:
        version_check = True
        q = q.with_lockmode(lockmode)
    elif query._for_update_arg is not None:
        version_check = True
        q._for_update_arg = query._for_update_arg
    else:
        version_check = False

    q._get_options(
        populate_existing=bool(refresh_state),
        version_check=version_check,
        only_load_props=only_load_props,
        refresh_state=refresh_state)
    q._order_by = None

    try:
        return q.one()
    except orm_exc.NoResultFound:
        return None


def _setup_entity_query(
    context, mapper, query_entity,
        path, adapter, column_collection,
        with_polymorphic=None, only_load_props=None,
        polymorphic_discriminator=None, **kw):

    if with_polymorphic:
        poly_properties = mapper._iterate_polymorphic_properties(
            with_polymorphic)
    else:
        poly_properties = mapper._polymorphic_properties

    quick_populators = {}

    path.set(
        context.attributes,
        "memoized_setups",
        quick_populators)

    for value in poly_properties:
        if only_load_props and \
                value.key not in only_load_props:
            continue
        value.setup(
            context,
            query_entity,
            path,
            adapter,
            only_load_props=only_load_props,
            column_collection=column_collection,
            memoized_populators=quick_populators,
            **kw
        )

    if polymorphic_discriminator is not None and \
        polymorphic_discriminator \
            is not mapper.polymorphic_on:

        if adapter:
            pd = adapter.columns[polymorphic_discriminator]
        else:
            pd = polymorphic_discriminator
        column_collection.append(pd)


def load_scalar_attributes(mapper, state, attribute_names):
    """initiate a column-based attribute refresh operation."""

    # assert mapper is _state_mapper(state)
    session = state.session
    if not session:
        raise orm_exc.DetachedInstanceError(
            "Instance %s is not bound to a Session; "
            "attribute refresh operation cannot proceed" %
            (state_str(state)))

    has_key = bool(state.key)

    result = False

    if mapper.inherits and not mapper.concrete:
        # because we are using Core to produce a select() that we
        # pass to the Query, we aren't calling setup() for mapped
        # attributes; in 1.0 this means deferred attrs won't get loaded
        # by default
        statement = mapper._optimized_get_statement(state, attribute_names)
        if statement is not None:
            result = load_on_ident(
                session.query(mapper).
                options(
                    strategy_options.Load(mapper).undefer("*")
                ).from_statement(statement),
                None,
                only_load_props=attribute_names,
                refresh_state=state
            )

    if result is False:
        if has_key:
            identity_key = state.key
        else:
            # this codepath is rare - only valid when inside a flush, and the
            # object is becoming persistent but hasn't yet been assigned
            # an identity_key.
            # check here to ensure we have the attrs we need.
            pk_attrs = [mapper._columntoproperty[col].key
                        for col in mapper.primary_key]
            if state.expired_attributes.intersection(pk_attrs):
                raise sa_exc.InvalidRequestError(
                    "Instance %s cannot be refreshed - it's not "
                    " persistent and does not "
                    "contain a full primary key." % state_str(state))
            identity_key = mapper._identity_key_from_state(state)

        if (_none_set.issubset(identity_key) and
                not mapper.allow_partial_pks) or \
                _none_set.issuperset(identity_key):
            util.warn_limited(
                "Instance %s to be refreshed doesn't "
                "contain a full primary key - can't be refreshed "
                "(and shouldn't be expired, either).",
                state_str(state))
            return

        result = load_on_ident(
            session.query(mapper),
            identity_key,
            refresh_state=state,
            only_load_props=attribute_names)

    # if instance is pending, a refresh operation
    # may not complete (even if PK attributes are assigned)
    if has_key and result is None:
        raise orm_exc.ObjectDeletedError(state)
