# orm/loading_speedups.pyx
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from __future__ import absolute_import

from .. import util
from . import attributes, exc as orm_exc

from .util import _none_set, state_str

from .base import _SET_DEFERRED_EXPIRED, _DEFER_FOR_STATE
import collections


def _instance_processor(
        mapper, context, result, path, adapter,
        only_load_props=None, refresh_state=None,
        polymorphic_discriminator=None,
        _polymorphic_from=None):
    """Produce a mapper level row processor callable
       which processes rows into mapped instances."""

    # note that this method, most of which exists in a closure
    # called _instance(), resists being broken out, as
    # attempts to do so tend to add significant function
    # call overhead.  _instance() is the most
    # performance-critical section in the whole ORM.

    pk_cols = mapper.primary_key

    if adapter:
        pk_cols = [adapter.columns[c] for c in pk_cols]

    identity_class = mapper._identity_class

    populators = collections.defaultdict(list)

    props = mapper._prop_set
    if only_load_props is not None:
        props = props.intersection(
            mapper._props[k] for k in only_load_props)

    quick_populators = path.get(
        context.attributes, "memoized_setups", _none_set)

    for prop in props:
        if prop in quick_populators:
            # this is an inlined path just for column-based attributes.
            col = quick_populators[prop]
            if col is _DEFER_FOR_STATE:
                populators["new"].append(
                    (prop.key, prop._deferred_column_loader))
            elif col is _SET_DEFERRED_EXPIRED:
                # note that in this path, we are no longer
                # searching in the result to see if the column might
                # be present in some unexpected way.
                populators["expire"].append((prop.key, False))
            else:
                if adapter:
                    col = adapter.columns[col]
                getter = result._getter(col)
                if getter:
                    populators["quick"].append((prop.key, getter))
                else:
                    # fall back to the ColumnProperty itself, which
                    # will iterate through all of its columns
                    # to see if one fits
                    prop.create_row_processor(
                        context, path, mapper, result, adapter, populators)
        else:
            prop.create_row_processor(
                context, path, mapper, result, adapter, populators)

    propagate_options = context.propagate_options
    if propagate_options:
        load_path = context.query._current_path + path \
            if context.query._current_path.path else path

    session_identity_map = context.session.identity_map

    populate_existing = context.populate_existing or mapper.always_refresh
    load_evt = bool(mapper.class_manager.dispatch.load)
    refresh_evt = bool(mapper.class_manager.dispatch.refresh)
    instance_state = attributes.instance_state
    instance_dict = attributes.instance_dict
    session_id = context.session.hash_key
    version_check = context.version_check
    runid = context.runid

    if refresh_state:
        refresh_identity_key = refresh_state.key
        if refresh_identity_key is None:
            # super-rare condition; a refresh is being called
            # on a non-instance-key instance; this is meant to only
            # occur within a flush()
            refresh_identity_key = \
                mapper._identity_key_from_state(refresh_state)
    else:
        refresh_identity_key = None

    if mapper.allow_partial_pks:
        is_not_primary_key = _none_set.issuperset
    else:
        is_not_primary_key = _none_set.intersection

    def _instance(row):

        # determine the state that we'll be populating
        if refresh_identity_key:
            # fixed state that we're refreshing
            state = refresh_state
            instance = state.obj()
            dict_ = instance_dict(instance)
            isnew = state.runid != runid
            currentload = True
            loaded_instance = False
        else:
            # look at the row, see if that identity is in the
            # session, or we have to create a new one
            identitykey = (
                identity_class,
                tuple([row[column] for column in pk_cols])
            )

            instance = session_identity_map.get(identitykey)

            if instance is not None:
                # existing instance
                state = instance_state(instance)
                dict_ = instance_dict(instance)

                isnew = state.runid != runid
                currentload = not isnew
                loaded_instance = False

                if version_check and not currentload:
                    _validate_version_id(mapper, state, dict_, row, adapter)

            else:
                # create a new instance

                # check for non-NULL values in the primary key columns,
                # else no entity is returned for the row
                if is_not_primary_key(identitykey[1]):
                    return None

                isnew = True
                currentload = True
                loaded_instance = True

                instance = mapper.class_manager.new_instance()

                dict_ = instance_dict(instance)
                state = instance_state(instance)
                state.key = identitykey

                # attach instance to session.
                state.session_id = session_id
                session_identity_map._add_unpresent(state, identitykey)

        # populate.  this looks at whether this state is new
        # for this load or was existing, and whether or not this
        # row is the first row with this identity.
        if currentload or populate_existing:
            # full population routines.  Objects here are either
            # just created, or we are doing a populate_existing

            if isnew and propagate_options:
                state.load_options = propagate_options
                state.load_path = load_path

            _populate_full(
                context, row, state, dict_, isnew,
                loaded_instance, populate_existing, populators)

            if isnew:
                if loaded_instance and load_evt:
                    state.manager.dispatch.load(state, context)
                elif refresh_evt:
                    state.manager.dispatch.refresh(
                        state, context, only_load_props)

                if populate_existing or state.modified:
                    if refresh_state and only_load_props:
                        state._commit(dict_, only_load_props)
                    else:
                        state._commit_all(dict_, session_identity_map)

        else:
            # partial population routines, for objects that were already
            # in the Session, but a row matches them; apply eager loaders
            # on existing objects, etc.
            unloaded = state.unloaded
            isnew = state not in context.partials

            if not isnew or unloaded or populators["eager"]:
                # state is having a partial set of its attributes
                # refreshed.  Populate those attributes,
                # and add to the "context.partials" collection.

                to_load = _populate_partial(
                    context, row, state, dict_, isnew,
                    unloaded, populators)

                if isnew:
                    if refresh_evt:
                        state.manager.dispatch.refresh(
                            state, context, to_load)

                    state._commit(dict_, to_load)

        return instance

    if mapper.polymorphic_map and not _polymorphic_from and not refresh_state:
        # if we are doing polymorphic, dispatch to a different _instance()
        # method specific to the subclass mapper
        _instance = _decorate_polymorphic_switch(
            _instance, context, mapper, result, path,
            polymorphic_discriminator, adapter)

    return _instance


def _populate_full(
        context, row, state, dict_, isnew,
        loaded_instance, populate_existing, populators):
    if isnew:
        # first time we are seeing a row with this identity.
        state.runid = context.runid

        for key, getter in populators["quick"]:
            dict_[key] = getter(row)
        if populate_existing:
            for key, set_callable in populators["expire"]:
                dict_.pop(key, None)
                if set_callable:
                    state.expired_attributes.add(key)
        else:
            for key, set_callable in populators["expire"]:
                if set_callable:
                    state.expired_attributes.add(key)
        for key, populator in populators["new"]:
            populator(state, dict_, row)
        for key, populator in populators["delayed"]:
            populator(state, dict_, row)
    else:
        # have already seen rows with this identity.
        for key, populator in populators["existing"]:
            populator(state, dict_, row)


def _populate_partial(
        context, row, state, dict_, isnew,
        unloaded, populators):
    if not isnew:
        to_load = context.partials[state]
        for key, populator in populators["existing"]:
            if key in to_load:
                populator(state, dict_, row)
    else:
        to_load = unloaded
        context.partials[state] = to_load

        for key, getter in populators["quick"]:
            if key in to_load:
                dict_[key] = getter(row)
        for key, set_callable in populators["expire"]:
            if key in to_load:
                dict_.pop(key, None)
                if set_callable:
                    state.expired_attributes.add(key)
        for key, populator in populators["new"]:
            if key in to_load:
                populator(state, dict_, row)
        for key, populator in populators["delayed"]:
            if key in to_load:
                populator(state, dict_, row)
    for key, populator in populators["eager"]:
        if key not in unloaded:
            populator(state, dict_, row)

    return to_load


def _validate_version_id(mapper, state, dict_, row, adapter):

    version_id_col = mapper.version_id_col

    if version_id_col is None:
        return

    if adapter:
        version_id_col = adapter.columns[version_id_col]

    if mapper._get_state_attr_by_column(
            state, dict_, mapper.version_id_col) != row[version_id_col]:
        raise orm_exc.StaleDataError(
            "Instance '%s' has version id '%s' which "
            "does not match database-loaded version id '%s'."
            % (state_str(state), mapper._get_state_attr_by_column(
               state, dict_, mapper.version_id_col),
               row[version_id_col]))


def _decorate_polymorphic_switch(
        instance_fn, context, mapper, result, path,
        polymorphic_discriminator, adapter):
    if polymorphic_discriminator is not None:
        polymorphic_on = polymorphic_discriminator
    else:
        polymorphic_on = mapper.polymorphic_on
    if polymorphic_on is None:
        return instance_fn

    if adapter:
        polymorphic_on = adapter.columns[polymorphic_on]

    def configure_subclass_mapper(discriminator):
        try:
            sub_mapper = mapper.polymorphic_map[discriminator]
        except KeyError:
            raise AssertionError(
                "No such polymorphic_identity %r is defined" %
                discriminator)
        else:
            if sub_mapper is mapper:
                return None

            return _instance_processor(
                sub_mapper, context, result,
                path, adapter, _polymorphic_from=mapper)

    polymorphic_instances = util.PopulateDict(
        configure_subclass_mapper
    )

    def polymorphic_instance(row):
        discriminator = row[polymorphic_on]
        if discriminator is not None:
            _instance = polymorphic_instances[discriminator]
            if _instance:
                return _instance(row)
        return instance_fn(row)
    return polymorphic_instance

