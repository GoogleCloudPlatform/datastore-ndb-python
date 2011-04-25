"""Model and Property classes and associated stuff.

A model class represents the structure of entities stored in the
datastore.  Applications define model classes to indicate the
structure of their entities, then instantiate those model classes
to create entities.

All model classes must inherit (directly or indirectly) from Model.
Through the magic of metaclasses, straightforward assignments in the
model class definition can be used to declare the model's structure:

  class Person(Model):
    name = StringProperty()
    age = IntegerProperty()

We can now create a Person entity and write it to the datastore:

  p = Person(name='Arthur Dent', age=42)
  k = p.put()

The return value from put() is a Key (see the documentation for
ndb/key.py), which can be used to retrieve the same entity later:

  p2 = k.get()
  assert p2 == p

To update an entity, simple change its attributes and write it back
(note that this doesn't change the key):

  p2.name = 'Arthur Philip Dent'
  p2.put()

We can also delete an entity (by using the key):

  k.delete()

The property definitions in the class body tell the system the names
and the types of the fields to be stored in the datastore, whether
they must be indexed, their default value, and more.

Many different Property types exist.  Most are indexed by default, the
exceptions indicated in the list below:

- StringProperty: a short text string, limited to 500 bytes

- TextProperty: an unlimited text string; unindexed

- BlobProperty: an unlimited byte string; unindexed

- IntegerProperty: a 64-bit signed integer

- FloatProperty: a double precision floating point number

- BooleanProperty: a bool value

- DateTimeProperty: a datetime object.  Note: App Engine always uses
  UTC as the timezone

- DateProperty: a date object

- TimeProperty: a time object

- GeoPtProperty: a geographical location, i.e. (latitude, longitude)

- KeyProperty: a datastore Key value

- UserProperty: a User object (for backwards compatibility only)

- StructuredProperty: a field that is itself structured like an
  entity; see below for more details

- LocalStructuredProperty: like StructuredProperty but the on-disk
  representation is an opaque blob; unindexed

- ComputedProperty: a property whose value is computed from other
  properties by a user-defined function.  The property value is
  written to the datastore so that it can be used in queries, but the
  value from the datastore is not used when the entity is read back

- GenericProperty: a property whose type is not constrained; mostly
  used by the Expando class (see below) but also usable explicitly

Most Property classes have similar constructor signatures.  They
accept several optional keyword arguments:

- name=<string>: the name used to store the property value in the
  datastore.  Unlike the following options, this may also be given as
  a positional argument

- indexed=<bool>: indicates whether the property should be indexed
  (allowing queries on this property's value)

- repeated=<bool>: indicates that this property can have multiple
  values in the same entity.

- required=<bool>: indicates that this property must be given a value

- default=<value>: a default value if no explicit value is given

- choices=<list of values>: a list or tuple of allowable values

- validator=<function>: a general-purpose validation function.  It
  will be called with two arguments (prop, value) and should either
  return the validated value or raise an exception.  It is also
  allowed for the function to modify the value, but calling it again
  on the modified value should not modify the value further.  (For
  example: a validator that returns value.strip() or value.lower() is
  fine, but one that returns value + '$' is not.)

The repeated, required and default options are mutually exclusive: a
repeated property cannot be required nor can it specify a default
value (the default is always an empty list and an empty list is always
an allowed value), and a required property cannot have a default.

Some property types have additional arguments.  Some property types
do not support all options.

Repeated properties are always represented as Python lists; if there
is only one value, the list has only one element.  When a new list is
assigned to a repeated property, all elements of the list are
validated.  Since it is also possible to mutate lists in place,
repeated properties are re-validated before they are written to the
datastore.

No validation happens when an entity is read from the datastore;
however property values read that have the wrong type (e.g. a string
value for an IntegerProperty) are ignored.

For non-repeated properties, None is always a possible value, and no
validation is called when the value is set to None.  However for
required properties, writing the entity to the datastore requires
the value to be something other than None (and valid).

The StructuredProperty is different from most other properties; it
lets you define a sub-structure for your entities.  The substructure
itself is defined using a model class, and the attribute value is an
instance of that model class.  However it is not stored in the
datastore as a separate entity; instead, its attribute values are
included in the parent entity using a naming convention (the name of
the structured attribute followed by a dot followed by the name of the
subattribute).  For example:

  class Address(Model):
    street = StringProperty()
    city = StringProperty()

  class Person(Model):
    name = StringProperty()
    address = StructuredProperty(Address)

  p = Person(name='Harry Potter',
             address=Address(street='4 Privet Drive',
                             city='Little Whinging'))
  k.put()

This would write a single 'Person' entity with three attributes (as
you could verify using the Datastore Viewer in the Admin Console):

  name = 'Harry Potter'
  address.street = '4 Privet Drive'
  address.city = 'Little Whinging'

Structured property types can be nested arbitrarily deep, but in a
hierarchy of nested structured property types, only one level can have
the repeated flag set.  It is fine to have multiple structured
properties referencing the same model class.

It is also fine to use the same model class both as a top-level entity
class and as for a structured property; however queries for the model
class will only return the top-level entities.

The LocalStructuredProperty works similar to StructuredProperty on the
Python side.  For example:

  class Address(Model):
    street = StringProperty()
    city = StringProperty()

  class Person(Model):
    name = StringProperty()
    address = LocalStructuredProperty(Address)

  p = Person(name='Harry Potter',
             address=Address(street='4 Privet Drive',
                             city='Little Whinging'))
  k.put()

However the data written to the datastore is different; it writes a
'Person' entity with a 'name' attribute as before and a single
'address' attribute whose value is a blob which encodes the Address
value (using the standard"protocol buffer" encoding).

Sometimes the set of properties is not known ahead of time.  In such
cases you can use the Expando class.  This is a Model subclass that
creates properties on the fly, both upon assignment and when loading
an entity from the datastore.  For example:

  class SuperPerson(Expando):
    name = StringProperty()
    superpower = StringProperty()

  razorgirl = SuperPerson(name='Molly Millions',
                          superpower='bionic eyes, razorblade hands',
                          rasta_name='Steppin\' Razor',
                          alt_name='Sally Shears')
  elastigirl = SuperPerson(name='Helen Parr',
                           superpower='stretchable body')
  elastigirl.max_stretch = 30  # Meters

You can inspect the properties of an expando instance using the
_properties attribute:

  >>> print razorgirl._properties.keys()
  ['rasta_name', 'name', 'superpower', 'alt_name']
  >>> print elastigirl._properties
  {'max_stretch': GenericProperty('max_stretch'),
   'name': StringProperty('name'),
   'superpower': StringProperty('superpower')}

Note: this property exists for plain Model instances too; it is just
not as interesting for those.

The Model class offers basic query support.  You can create a Query
object by calling the query() class method.  Iterating over a Query
object returns the entities matching the query one at a time.

Query objects are fully described in the docstring for query.py, but
there is one handy shortcut that is only available through
Model.query(): positional arguments are interpreted as filter
expressions which are combined through an AND operator.  For example:

  Person.query(Person.name == 'Harry Potter', Person.age >= 11)

is equivalent to:

  Person.query().filter(Person.name == 'Harry Potter', Person.age >= 11)

Keyword arguments passed to .query() are passed along to the Query()
constructor.

It is possible to query for field values of stuctured properties.  For
example:

  qry = Person.query(Person.address.city == 'London')

A number of top-level functions also live in this module:

- transaction() runs a function inside a transaction
- get_multi() reads multiple entities at once
- put_multi() writes multiple entities at once
- delete_multi() deletes multiple entities at once

All these have a corresponding *_async() variant as well.
The *_multi_async() functions return a list of Futures.

And finally these (without async variants):

- in_transaction() tests whether you are currently running in a transaction
- @transactional decorates functions that should be run in a transaction
"""

__author__ = 'guido@google.com (Guido van Rossum)'

# TODO: Change asserts to better exceptions.
# TODO: Add PolyModel.

import copy
import datetime
import logging
import zlib

from google.appengine.api import datastore_errors
from google.appengine.api import datastore_types
from google.appengine.api import users
from google.appengine.datastore import datastore_query
from google.appengine.datastore import datastore_rpc
from google.appengine.datastore import entity_pb

from ndb import utils

# NOTE: Don't use "from ndb import key"; key is a common local variable name.
import ndb.key
Key = ndb.key.Key  # For export.

# NOTE: Property and Error classes are added later.
__all__ = ['Key', 'ModelAdapter', 'ModelKey', 'MetaModel', 'Model', 'Expando',
           'BlobKey', 'GeoPt',
           'transaction', 'transaction_async',
           'in_transaction', 'transactional',
           'get_multi', 'get_multi_async',
           'put_multi', 'put_multi_async',
           'delete_multi', 'delete_multi_async',
           ]


BlobKey = datastore_types.BlobKey
GeoPt = datastore_types.GeoPt


class KindError(datastore_errors.BadValueError):
  """Raised when an implementation for a kind can't be found."""


class ComputedPropertyError(datastore_errors.Error):
  """Raised when attempting to assign a value to a computed property."""


class ModelAdapter(datastore_rpc.AbstractAdapter):
  """Conversions between 'our' Key and Model classes and protobufs.

  This is needed to construct a Connection object, which in turn is
  needed to construct a Context object.

  See the base class docstring for more info about the signatures.
  """

  def __init__(self, default_model=None):
    """Constructor.

    Args:
      default_model: If an implementation for the kind cannot be found, use this
        model class. If none is specified, an exception will be thrown
        (default).
    """
    self.default_model = default_model

  def pb_to_key(self, pb):
    return Key(reference=pb)

  def key_to_pb(self, key):
    return key.reference()

  def pb_to_entity(self, pb):
    kind = None
    if pb.has_key():
      # TODO: Fix the inefficiency here: we extract the key just so we
      # can get the kind just so we can find the intended model class,
      # but the key is extracted again and stored in the entity by _from_pb().
      key = Key(reference=pb.key())
      kind = key.kind()
    modelclass = Model._kind_map.get(kind, self.default_model)
    if modelclass is None:
      raise KindError("No implementation found for kind '%s'" % kind)
    return modelclass._from_pb(pb)

  def entity_to_pb(self, ent):
    pb = ent._to_pb()
    return pb


def make_connection(config=None, default_model=None):
  """Create a new Connection object with the right adapter.

  Optionally you can pass in a datastore_rpc.Configuration object.
  """
  return datastore_rpc.Connection(
      adapter=ModelAdapter(default_model),
      config=config)


class Property(object):
  """A class describing a typed, persisted attribute of a datastore entity.

  Not to be confused with Python's 'property' built-in.

  This is just a base class; there are specific subclasses that
  describe Properties of various types (and GenericProperty which
  describes a dynamically typed Property).

  All special Property attributes, even those considered 'public',
  have names starting with an underscore, because StructuredProperty
  uses the non-underscore attribute namespace to refer to nested
  Property names; this is essential for specifying queries on
  subproperties (see the module docstring).
  """

  # TODO: Separate 'simple' properties from base Property class

  _code_name = None
  _name = None
  _indexed = True
  _repeated = False
  _required = False
  _default = None
  _choices = None
  _validator = None

  _attributes = ['_name', '_indexed', '_repeated', '_required', '_default',
                 '_choices', '_validator']
  _positional = 1

  @datastore_rpc._positional(1 + _positional)
  def __init__(self, name=None, indexed=None, repeated=None,
               required=None, default=None, choices=None, validator=None):
    """Constructor.  For arguments see the module docstring."""
    if name is not None:
      assert '.' not in name  # The '.' is used elsewhere.
      self._name = name
    if indexed is not None:
      self._indexed = indexed
    if repeated is not None:
      self._repeated = repeated
    if required is not None:
      self._required = required
    if default is not None:
      self._default = default
    assert (bool(self._repeated) +
            bool(self._required) +
            (self._default is not None)) <= 1  # Allow at most one of these
    if choices is not None:
      assert isinstance(choices, (tuple, list))
      self._choices = tuple(choices)
    if validator is not None:
      # The validator is called as follows:
      #   value = validator(prop, value)
      # It should return the value to be used, or raise an exception.
      # It should be idempotent, i.e. calling it a second time should
      # not further modify the value.  So a validator that returns e.g.
      # value.lower() or value.strip() is fine, but one that returns
      # value + '$' is not.
      assert callable(validator)
      self._validator = validator

  def __repr__(self):
    """Return a compact unambiguous string representation."""
    args = []
    cls = self.__class__
    for i, attr in enumerate(self._attributes):
      val = getattr(self, attr)
      if val is not getattr(cls, attr):
        if isinstance(val, type):
          s = val.__name__
        else:
          s = repr(val)
        if i >= cls._positional:
          if attr.startswith('_'):
            attr = attr[1:]
          s = '%s=%s' % (attr, s)
        args.append(s)
    s = '%s(%s)' % (self.__class__.__name__, ', '.join(args))
    return s

  def _datastore_type(self, value):
    return value

  def _comparison(self, op, value):
    """Internal helper for comparison operators.

    Args:
      op: The operator ('=', '<' etc.).

    Returns:
      A FilterNode instance representing the requested comparison.
    """
    from ndb.query import FilterNode  # Import late to avoid circular imports.
    if value is not None:
      # TODO: Allow query.Binding instances?
      value = self._validate(value)
    return FilterNode(self._name, op, self._datastore_type(value))

  # Comparison operators on Property instances don't compare the
  # properties; instead they return FilterNode instances that can be
  # used in queries.  See the module docstrings above and in query.py
  # for details on how these can be used.

  def __eq__(self, value):
    """Return a FilterNode instance representing the '=' comparison."""
    return self._comparison('=', value)

  def __ne__(self, value):
    """Return a FilterNode instance representing the '!=' comparison."""
    return self._comparison('!=', value)

  def __lt__(self, value):
    """Return a FilterNode instance representing the '<' comparison."""
    return self._comparison('<', value)

  def __le__(self, value):
    """Return a FilterNode instance representing the '<=' comparison."""
    return self._comparison('<=', value)

  def __gt__(self, value):
    """Return a FilterNode instance representing the '>' comparison."""
    return self._comparison('>', value)

  def __ge__(self, value):
    """Return a FilterNode instance representing the '>=' comparison."""
    return self._comparison('>=', value)

  def _IN(self, value):
    """Comparison operator for the 'in' comparison operator.

    The Python 'in' operator cannot be overloaded in the way we want
    to, so we define a method.  For example:

      Employee.query(Employee.rank.IN([4, 5, 6]))

    Note that the method is called ._IN() but may normally be invoked
    as .IN(); ._IN() is provided for the case you have a
    StructuredProperty with a model that has a Property named IN.
    """
    from ndb.query import FilterNode  # Import late to avoid circular imports.
    if not isinstance(value, (list, tuple)):
      raise datastore_errors.BadArgumentError('Expected list or tuple, got %r' %
                                              (value,))
    values = []
    for val in value:
      if val is not None:
        val is self._validate(val)
        values.append(val)
    return FilterNode(self._name, 'in', values)
  IN = _IN

  def __neg__(self):
    """Return a descending sort order on this Property.

    For example:

      Employee.query().order(-Employee.rank)
    """
    return datastore_query.PropertyOrder(
      self._name, datastore_query.PropertyOrder.DESCENDING)

  def __pos__(self):
    """Return an ascending sort order on this Property.

    Note that this is redundant but provided for consistency with
    __neg__.  For example, the following two are equivalent:

      Employee.query().order(+Employee.rank)
      Employee.query().order(Employee.rank)
    """
    return datastore_query.PropertyOrder(self._name)

  # TODO: Explain somewhere that None is never validated.
  # TODO: What if a custom validator returns None?
  # TODO: What if a custom validator wants to coerce a type that the
  # built-in validator for a given class does not allow?

  def _validate(self, value):
    """Template method to validate and possibly modify the value.

    This is intended to be overridden by Property subclasses.  It
    should return the value either unchanged or modified in an
    idempotent way, or raise an exception to indicate that the value
    is invalid.  By convention the exception raised is BadValueError.

    Note that for a repeated Property this function should be called
    for each item in the list, not for the list as a whole.
    """
    return value

  def _do_validate(self, value):
    """Call all validations on the value.

    This first calls self._validate(), then the custom validator
    function, and finally checks the choices.  It returns the value,
    possibly modified in an idempotent way, or raises an exception.

    Note that for a repeated Property this function should be called
    for each item in the list, not for the list as a whole.
    """
    value = self._validate(value)
    if self._validator is not None:
      value = self._validator(self, value)
    if self._choices is not None:
      if value not in self._choices:
        raise datastore_errors.BadValueError(
          'Value %r for property %s is not an allowed choice' %
          (value, self._name))
    return value

  def _fix_up(self, code_name):
    """Internal helper called to tell the property its name.

    This is called by _fix_up_properties() which is called by
    MetaModel when finishing the construction of a Model subclass.
    The name passed in is the name of the class attribute to which the
    Property is assigned (a.k.a. the code name).  Note that this means
    that each Property instance must be assigned to (at most) one
    class attribute.  E.g. to declare three strings, you must call
    StringProperty() three times, you cannot write

      foo = bar = baz = StringProperty()
    """
    self._code_name = code_name
    if self._name is None:
      self._name = code_name

  def _store_value(self, entity, value):
    """Internal helper to store a value in an entity for a Property.

    This assumes validation has already taken place.  For a repeated
    Property the value should be a list.
    """
    entity._values[self._name] = value

  def _set_value(self, entity, value):
    """Internal helper to set a value in an entity for a Property.

    This performs validation first.  For a repeated Property the value
    should be a list.
    """
    if self._repeated:
      if not isinstance(value, (list, tuple)):
        raise datastore_errors.BadValueError('Expected list or tuple, got %r' %
                                             (value,))
      values = []
      for val in value:
        val = self._do_validate(val)
        values.append(val)
    else:
      if value is not None:
        value = self._do_validate(value)
    self._store_value(entity, value)

  def _has_value(self, entity):
    """Internal helper to ask if the entity has a value for this Property."""
    return self._name in entity._values

  def _retrieve_value(self, entity):
    """Internal helper to retrieve the value for this Property from an entity.

    This returns None if no value is set.  For a repeated Property
    this returns a list if a value is set, otherwise None.
    """
    return entity._values.get(self._name, self._default)

  def _get_value(self, entity):
    """Internal helper to get the value for this Property from an entity.

    For a repeated Property this initializes the value to an empty
    list if it is not set.
    """
    value = self._retrieve_value(entity)
    if value is None and self._repeated:
      value = []
      self._store_value(entity, value)
    return value

  def _delete_value(self, entity):
    """Internal helper to delete the value for this Property from an entity.

    Note that if no value exists this is a no-op; deleted values will
    not be serialized but requesting their value will return None (or
    an empty list in the case of a repeated Property).
    """
    if self._name in entity._values:
      del entity._values[self._name]

  def _is_initialized(self, entity):
    """Internal helper to ask if the entity has a value for this Property.

    This returns False if a value is stored but it is None.
    """
    return not self._required or (self._has_value(entity) and
                                  self._get_value(entity) is not None)

  def __get__(self, entity, cls=None):
    """Descriptor protocol: get the value from the entity."""
    if entity is None:
      return self  # __get__ called on class
    return self._get_value(entity)

  def __set__(self, entity, value):
    """Descriptor protocol: set the value on the entity."""
    self._set_value(entity, value)

  def __delete__(self, entity):
    """Descriptor protocol: delete the value from the entity."""
    self._delete_value(entity)

  def _serialize(self, entity, pb, prefix='', parent_repeated=False):
    """Internal helper to serialize this property to a protocol buffer.

    Subclasses may override this method.

    Args:
      entity: The entity, a Model (subclass) instance.
      pb: The protocol buffer, an EntityProto instance.
      prefix: Optional name prefix used for StructuredProperty
        (if present, must end in '.').
      parent_repeated: True if the parent (or an earlier ancestor)
        is a repeated Property.
    """
    value = self._retrieve_value(entity)
    if value is None and self._repeated:
      value = []
    elif not isinstance(value, list):
      value = [value]
    for val in value:
      if self._repeated:
        # Re-validate repeated values, since the user could have
        # appended values to the list, bypassing validation.
        val = self._do_validate(val)
      if self._indexed:
        p = pb.add_property()
      else:
        p = pb.add_raw_property()
      p.set_name(prefix + self._name)
      p.set_multiple(self._repeated or parent_repeated)
      v = p.mutable_value()
      if val is not None:
        self._db_set_value(v, p, val)

  def _deserialize(self, entity, p, depth=1):
    """Internal helper to deserialize this property from a protocol buffer.

    Subclasses may override this method.

    Args:
      entity: The entity, a Model (subclass) instance.
      p: A Property Message object (a protocol buffer).
      depth: Optional nesting depth, default 1 (unused here, but used
        by some subclasses that override this method).
    """
    v = p.value()
    val = self._db_get_value(v, p)
    if self._repeated:
      if self._has_value(entity):
        value = self._retrieve_value(entity)
        if not isinstance(value, list):
          value = [value]
        value.append(val)
      else:
        value = [val]
    else:
      if not self._has_value(entity):
        value = val
      else:
        oldval = self._retrieve_value(entity)
        # Maybe upgrade to a list property.  Or ignore null.
        if val is None:
          value = oldval
        elif oldval is None:
          value = val
        elif isinstance(oldval, list):
          oldval.append(val)
          value = oldval
        else:
          value = [oldval, val]
    self._store_value(entity, value)


def _validate_key(value, entity=None):
  if not isinstance(value, Key):
    # TODO: BadKeyError.
    raise datastore_errors.BadValueError('Expected Key, got %r' % value)
  if entity and entity.__class__ not in (Model, Expando):
    if value.kind() != entity._get_kind():
      raise KindError('Expected Key kind to be %s; received %s' %
                      (entity._get_kind(), value.kind()))
  return value


class ModelKey(Property):
  """Special property to store the Model key."""

  def __init__(self):
    self._name = '__key__'

  def _datastore_type(self, value):
    return datastore_types.Key(value.urlsafe())

  def _comparison(self, op, value):
    if value is not None:
      return super(ModelKey, self)._comparison(op, value)
    raise datastore_errors.BadValueError(
        "__key__ filter query can't be compared to None")

  # TODO: Support IN().

  def _validate(self, value):
    return _validate_key(value)

  def _set_value(self, entity, value):
    """Setter for key attribute."""
    if value is not None:
      value = _validate_key(value, entity=entity)
    entity._key = value

  def _get_value(self, entity):
    """Getter for key attribute."""
    return entity._key

  def _delete_value(self, entity):
    """Deleter for key attribute."""
    entity._key = None


class BooleanProperty(Property):
  """A Property whose value is a Python bool."""
  # TODO: Allow int/long values equal to 0 or 1?

  def _validate(self, value):
    if not isinstance(value, bool):
      raise datastore_errors.BadValueError('Expected bool, got %r' %
                                           (value,))
    return value

  def _db_set_value(self, v, p, value):
    assert isinstance(value, bool), (self._name)
    v.set_booleanvalue(value)

  def _db_get_value(self, v, p):
    if not v.has_booleanvalue():
      return None
    # The booleanvalue field is an int32, so booleanvalue() returns an
    # int, hence the conversion.
    return bool(v.booleanvalue())


class IntegerProperty(Property):
  """A Property whose value is a Python int or long (or bool)."""

  def _validate(self, value):
    if not isinstance(value, (int, long)):
      raise datastore_errors.BadValueError('Expected integer, got %r' %
                                           (value,))
    return int(value)

  def _db_set_value(self, v, p, value):
    assert isinstance(value, (bool, int, long)), (self._name)
    v.set_int64value(value)

  def _db_get_value(self, v, p):
    if not v.has_int64value():
      return None
    return int(v.int64value())


class FloatProperty(Property):
  """A Property whose value is a Python float.

  Note: int, long and bool are also allowed.
  """

  def _validate(self, value):
    if not isinstance(value, (int, long, float)):
      raise datastore_errors.BadValueError('Expected float, got %r' %
                                           (value,))
    return float(value)

  def _db_set_value(self, v, p, value):
    assert isinstance(value, (bool, int, long, float)), (self._name)
    v.set_doublevalue(float(value))

  def _db_get_value(self, v, p):
    if not v.has_doublevalue():
      return None
    return v.doublevalue()


class StringProperty(Property):
  """A Property whose value is a text string."""
  # TODO: Enforce size limit when indexed.

  def _validate(self, value):
    if not isinstance(value, basestring):
      raise datastore_errors.BadValueError('Expected string, got %r' %
                                           (value,))
    # TODO: Always convert to Unicode?  But what if it's unconvertible?
    return value

  def _db_set_value(self, v, p, value):
    assert isinstance(value, basestring)
    if isinstance(value, unicode):
      value = value.encode('utf-8')
    v.set_stringvalue(value)
    if not self._indexed:
      p.set_meaning(entity_pb.Property.TEXT)

  def _db_get_value(self, v, p):
    if not v.has_stringvalue():
      return None
    raw = v.stringvalue()
    try:
      value = raw.decode('utf-8')
      return value
    except UnicodeDecodeError:
      return raw


class TextProperty(StringProperty):
  """An unindexed Property whose value is a text string of unlimited length."""
  # TODO: Maybe just use StringProperty(indexed=False)?

  _indexed = False

  def __init__(self, *args, **kwds):
    super(TextProperty, self).__init__(*args, **kwds)
    assert not self._indexed


class BlobProperty(Property):
  """A Property whose value is a byte string."""
  # TODO: Enforce size limit when indexed.

  _indexed = False

  def _validate(self, value):
    if not isinstance(value, str):
      raise datastore_errors.BadValueError('Expected 8-bit string, got %r' %
                                           (value,))
    return value

  def _db_set_value(self, v, p, value):
    assert isinstance(value, str)
    v.set_stringvalue(value)
    if self._indexed:
      p.set_meaning(entity_pb.Property.BYTESTRING)
    else:
      p.set_meaning(entity_pb.Property.BLOB)

  def _db_get_value(self, v, p):
    if not v.has_stringvalue():
      return None
    return v.stringvalue()


class GeoPtProperty(Property):
  """A Property whose value is a GeoPt."""

  def _validate(self, value):
    if not isinstance(value, GeoPt):
      raise datastore_errors.BadValueError('Expected GeoPt, got %r' %
                                           (value,))
    return value

  def _db_set_value(self, v, p, value):
    assert isinstance(value, GeoPt), (self._name)
    pv = v.mutable_pointvalue()
    pv.set_x(value.lat)
    pv.set_y(value.lon)

  def _db_get_value(self, v, p):
    if not v.has_pointvalue():
      return None
    pv = v.pointvalue()
    return GeoPt(pv.x(), pv.y())


def _unpack_user(v):
  """Internal helper to unpack a User value from a protocol buffer."""
  uv = v.uservalue()
  email = unicode(uv.email().decode('utf-8'))
  auth_domain = unicode(uv.auth_domain().decode('utf-8'))
  obfuscated_gaiaid = uv.obfuscated_gaiaid().decode('utf-8')
  obfuscated_gaiaid = unicode(obfuscated_gaiaid)

  federated_identity = None
  if uv.has_federated_identity():
    federated_identity = unicode(
        uv.federated_identity().decode('utf-8'))

  value = users.User(email=email,
                     _auth_domain=auth_domain,
                     _user_id=obfuscated_gaiaid,
                     federated_identity=federated_identity)
  return value


class UserProperty(Property):
  """A Property whose value is a User object.

  Note: this exists for backwards compatibility with existing
  datastore schemas only; we do not recommend storing User objects
  directly in the datastore, but instead recommend storing the
  user.user_id() value.
  """

  def _validate(self, value):
    if not isinstance(value, users.User):
      raise datastore_errors.BadValueError('Expected User, got %r' %
                                           (value,))
    return value

  def _db_set_value(self, v, p, value):
    datastore_types.PackUser(p.name(), value, v)

  def _db_get_value(self, v, p):
    return _unpack_user(v)


class KeyProperty(Property):
  """A Property whose value is a Key object."""
  # TODO: optionally check the kind (or maybe require this?)

  def _datastore_type(self, value):
    return datastore_types.Key(value.urlsafe())

  def _validate(self, value):
    if not isinstance(value, Key):
      raise datastore_errors.BadValueError('Expected Key, got %r' % (value,))
    # Reject incomplete keys.
    if not value.id():
      raise datastore_errors.BadValueError('Expected complete Key, got %r' %
                                           (value,))
    return value

  def _db_set_value(self, v, p, value):
    assert isinstance(value, Key)
    # See datastore_types.PackKey
    ref = value._reference()  # Don't copy
    rv = v.mutable_referencevalue()  # A Reference
    rv.set_app(ref.app())
    if ref.has_name_space():
      rv.set_name_space(ref.name_space())
    for elem in ref.path().element_list():
      rv.add_pathelement().CopyFrom(elem)

  def _db_get_value(self, v, p):
    if not v.has_referencevalue():
      return None
    ref = entity_pb.Reference()
    rv = v.referencevalue()
    if rv.has_app():
      ref.set_app(rv.app())
    if rv.has_name_space():
      ref.set_name_space(rv.name_space())
    path = ref.mutable_path()
    for elem in rv.pathelement_list():
      path.add_element().CopyFrom(elem)
    return Key(reference=ref)


class BlobKeyProperty(Property):
  """A Property whose value is a BlobKey object."""

  def _validate(self, value):
    if not isinstance(value, datastore_types.BlobKey):
      raise datastore_errors.BadValueError('Expected BlobKey, got %r' %
                                           (value,))
    return value

  def _db_set_value(self, v, p, value):
    assert isinstance(value, datastore_types.BlobKey)
    p.set_meaning(entity_pb.Property.BLOBKEY)
    v.set_stringvalue(str(value))

  def _db_get_value(self, v, p):
    if not v.has_stringvalue():
      return None
    return datastore_types.BlobKey(v.stringvalue())


# The Epoch (a zero POSIX timestamp).
_EPOCH = datetime.datetime.utcfromtimestamp(0)

class DateTimeProperty(Property):
  """A Property whose value is a datetime object.

  Note: Unlike Django, auto_now_add can be overridden by setting the
  value before writing the entity.  And unlike classic db, auto_now
  does not supply a default value.  Also unlike classic db, when the
  entity is written, the property values are updated to match what
  was written.  Finally, beware that this also updates the value in
  the in-process cache, *and* that auto_now_add may interact weirdly
  with transaction retries (a retry of a property with auto_now_add
  set will reuse the value that was set on the first try).
  """

  _attributes = Property._attributes + ['_auto_now', '_auto_now_add']

  @datastore_rpc._positional(1 + Property._positional)
  def __init__(self, name=None, auto_now=False, auto_now_add=False, **kwds):
    super(DateTimeProperty, self).__init__(name=name, **kwds)
    if self._repeated:
      assert not auto_now
      assert not auto_now_add
    self._auto_now = auto_now
    self._auto_now_add = auto_now_add

  def _validate(self, value):
    if not isinstance(value, datetime.datetime):
      raise datastore_errors.BadValueError('Expected datetime, got %r' %
                                           (value,))
    return value

  def _now(self):
    return datetime.datetime.now()

  def _serialize(self, entity, *rest):
    if (self._auto_now or
        (self._auto_now_add and self._retrieve_value(entity) is None)):
      value = self._now()
      self._store_value(entity, value)
    super(DateTimeProperty, self)._serialize(entity, *rest)

  def _db_set_value(self, v, p, value):
    assert isinstance(value, datetime.datetime)
    assert value.tzinfo is None
    dt = value - _EPOCH
    ival = dt.microseconds + 1000000 * (dt.seconds + 24*3600 * dt.days)
    v.set_int64value(ival)
    p.set_meaning(entity_pb.Property.GD_WHEN)

  def _db_get_value(self, v, p):
    if not v.has_int64value():
      return None
    ival = v.int64value()
    return _EPOCH + datetime.timedelta(microseconds=ival)


def _date_to_datetime(value):
  """Convert a date to a datetime for datastore storage.

  Args:
    value: A datetime.date object.

  Returns:
    A datetime object with time set to 0:00.
  """
  assert isinstance(value, datetime.date)
  return datetime.datetime(value.year, value.month, value.day)


def _time_to_datetime(value):
  """Convert a time to a datetime for datastore storage.

  Args:
    value: A datetime.time object.

  Returns:
    A datetime object with date set to 1970-01-01.
  """
  assert isinstance(value, datetime.time)
  return datetime.datetime(1970, 1, 1,
                           value.hour, value.minute, value.second,
                           value.microsecond)


class DateProperty(DateTimeProperty):
  """A Property whose value is a date object."""

  def _datastore_type(self, value):
    return _date_to_datetime(value)

  def _validate(self, value):
    if (not isinstance(value, datetime.date) or
        isinstance(value, datetime.datetime)):
      raise datastore_errors.BadValueError('Expected date, got %r' %
                                           (value,))
    return value

  def _now(self):
    return datetime.date.today()

  def _db_set_value(self, v, p, value):
    value = _date_to_datetime(value)
    super(DateProperty, self)._db_set_value(v, p, value)

  def _db_get_value(self, v, p):
    value = super(DateProperty, self)._db_get_value(v, p)
    return value.date()


class TimeProperty(DateTimeProperty):
  """A Property whose value is a time object."""

  def _datastore_type(self, value):
    return _time_to_datetime(value)

  def _validate(self, value):
    if not isinstance(value, datetime.time):
      raise datastore_errors.BadValueError('Expected time, got %r' %
                                           (value,))
    return value

  def _now(self):
    return datetime.datetime.now().time()

  def _db_set_value(self, v, p, value):
    value = _time_to_datetime(value)
    super(TimeProperty, self)._db_set_value(v, p, value)

  def _db_get_value(self, v, p):
    value = super(TimeProperty, self)._db_get_value(v, p)
    return value.time()


class StructuredProperty(Property):
  """A Property whose value is itself an entity.

  The values of the sub-entity are indexed and can be queried.

  See the module docstring for details.
  """

  _modelclass = None

  _attributes = ['_modelclass'] + Property._attributes
  _positional = 2

  @datastore_rpc._positional(1 + _positional)
  def __init__(self, modelclass, name=None, **kwds):
    super(StructuredProperty, self).__init__(name=name, **kwds)
    if self._repeated:
      assert not modelclass._has_repeated
    self._modelclass = modelclass

  def _fix_up(self, code_name):
    super(StructuredProperty, self)._fix_up(code_name)
    self._fix_up_nested_properties()

  def _fix_up_nested_properties(self):
    for name, prop in self._modelclass._properties.iteritems():
      prop_copy = copy.copy(prop)
      prop_copy._name = self._name + '.' + prop._name
      if isinstance(prop_copy, StructuredProperty):
        # Guard against simple recursive model definitions.
        # See model_test: testRecursiveStructuredProperty().
        # TODO: Guard against indirect recursion.
        if prop_copy._modelclass is not self._modelclass:
          prop_copy._fix_up_nested_properties()
      setattr(self, prop._code_name, prop_copy)

  def _comparison(self, op, value):
    if op != '=':
      raise datastore_errors.BadFilterError(
        'StructuredProperty filter can only use ==')
    # Import late to avoid circular imports.
    from ndb.query import FilterNode, ConjunctionNode, PostFilterNode
    value = self._validate(value)  # None is not allowed!
    filters = []
    for name, prop in value._properties.iteritems():
      val = prop._retrieve_value(value)
      if val is not None:
        filters.append(FilterNode(self._name + '.' + name, op, val))
    if not filters:
      raise datastore_errors.BadFilterError(
        'StructuredProperty filter without any values')
    if len(filters) == 1:
      return filters[0]
    filters.append(PostFilterNode(self._filter_func, value))
    return ConjunctionNode(filters)

  def _filter_func(self, value, entity):
    if isinstance(entity, Key):
      raise datastore_errors.BadQueryError(
        'StructuredProperty filter cannot be used with keys_only query')
    subentities = getattr(entity, self._code_name, None)
    if subentities is None:
      return False
    if not isinstance(subentities, list):
      subentities = [subentities]
    for subentity in subentities:
      for name, prop in value._properties.iteritems():
        val = prop._retrieve_value(value)
        if val is not None:
          if prop._retrieve_value(subentity) != val:
            break
      else:
        return True
    return False

  def _validate(self, value):
    if not isinstance(value, self._modelclass):
      raise datastore_errors.BadValueError('Expected %s instance, got %r' %
                                           (self._modelclass.__name__, value))
    return value

  def _serialize(self, entity, pb, prefix='', parent_repeated=False):
    # entity -> pb; pb is an EntityProto message
    value = self._retrieve_value(entity)
    if value is None:
      # TODO: Is this the right thing for queries?
      # Skip structured values that are None.
      return
    cls = self._modelclass
    if self._repeated:
      assert isinstance(value, list)
      values = value
    else:
      assert isinstance(value, cls)
      values = [value]
    for value in values:
      # TODO: Avoid re-sorting for repeated values.
      for name, prop in sorted(value._properties.iteritems()):
        prop._serialize(value, pb, prefix + self._name + '.',
                       self._repeated or parent_repeated)

  def _deserialize(self, entity, p, depth=1):
    if not self._repeated:
      subentity = self._retrieve_value(entity)
      if subentity is None:
        subentity = self._modelclass()
        self._store_value(entity, subentity)
      assert isinstance(subentity, self._modelclass)
      prop = subentity._get_property_for(p, depth=depth)
      prop._deserialize(subentity, p, depth + 1)
      return

    # The repeated case is more complicated.
    # TODO: Prove we won't get here for orphans.
    name = p.name()
    parts = name.split('.')
    assert len(parts) > depth, (depth, name, parts)
    next = parts[depth]
    prop = self._modelclass._properties.get(next)
    assert prop is not None  # QED

    values = self._retrieve_value(entity)
    if values is None:
      values = []
    elif not isinstance(values, list):
      values = [values]
    self._store_value(entity, values)
    # Find the first subentity that doesn't have a value for this
    # property yet.
    for sub in values:
      assert isinstance(sub, self._modelclass)
      if not prop._has_value(sub):
        subentity = sub
        break
    else:
      subentity = self._modelclass()
      values.append(subentity)
    prop._deserialize(subentity, p, depth + 1)


# A custom 'meaning' for compressed blobs.
_MEANING_URI_COMPRESSED = 'ZLIB'


class LocalStructuredProperty(Property):
  """Substructure that is serialized to an opaque blob.

  This looks like StructuredProperty on the Python side, but is
  written to the datastore as a single opaque blob.  It is not indexed
  and you cannot query for subproperties.  On the other hand, the
  on-disk representation is more efficient and can be made even more
  efficient by passing compressed=True, which compresses the blob
  data using gzip.
  """

  _indexed = False
  _compressed = False
  _modelclass = None

  _attributes = ['_modelclass'] + Property._attributes + ['_compressed']
  _positional = 2

  @datastore_rpc._positional(1 + _positional)
  def __init__(self, modelclass, name=None, compressed=False, **kwds):
    super(LocalStructuredProperty, self).__init__(name=name, **kwds)
    assert not self._indexed
    if self._repeated:
      assert not modelclass._has_repeated
    self._modelclass = modelclass
    self._compressed = compressed

  def _validate(self, value):
    # This is kind of a hack. Allow tuples because if the property comes from
    # datastore *and* is unchanged *and* the property has repeated=True,
    # _serialize() will call _do_validate() while the value is still a tuple.
    if not isinstance(value, (self._modelclass, tuple)):
      raise datastore_errors.BadValueError('Expected %s instance, got %r' %
                                           (self._modelclass.__name__, value))
    return value

  def _db_set_value(self, v, p, value):
    """Serializes the value to an entity_pb.

    The value stored in entity._values[self._name] can be either:

    - A tuple (serialized: bytes, compressed: bool), when the value comes
      from datastore. This is the serialized model and a flag indicating if it
      is compressed, used to lazily decompress and deserialize the property
      when it is first accessed.
    - An instance of self._modelclass, when the property value is set, or
      after it is lazily decompressed and deserialized on first access.
    """
    if isinstance(value, tuple):
      # Value didn't change and is still serialized, so we store it as it is.
      serialized, compressed = value
      assert compressed == self._compressed
    else:
      pb = value._to_pb()
      serialized = pb.Encode()
      compressed = self._compressed
      if compressed:
        p.set_meaning_uri(_MEANING_URI_COMPRESSED)
        serialized = zlib.compress(serialized)
    if compressed:
      # Use meaning_uri because setting meaning to something else that is not
      # BLOB or BYTESTRING will cause the value to be decoded from utf-8
      # in datastore_types.FromPropertyPb. This breaks the compressed string.
      p.set_meaning_uri(_MEANING_URI_COMPRESSED)
    p.set_meaning(entity_pb.Property.BLOB)
    v.set_stringvalue(serialized)

  def _db_get_value(self, v, p):
    if not v.has_stringvalue():
      return None
    # Return a tuple (serialized, bool) to be lazily processed later.
    return v.stringvalue(), p.meaning_uri() == _MEANING_URI_COMPRESSED

  def _decompress_unserialize_value(self, value):
    serialized, compressed = value
    if compressed:
      serialized = zlib.decompress(serialized)
    pb = entity_pb.EntityProto(serialized)
    return self._modelclass._from_pb(pb, set_key=False)

  def _get_value(self, entity):
    value = super(LocalStructuredProperty, self)._get_value(entity)
    if self._repeated:
      if value and isinstance(value[0], tuple):
        # Decompresses and deserializes each list item.
        # Reuse the original list, cleaning it first.
        values = list(value)
        del value[:]
        for v in values:
          value.append(self._decompress_unserialize_value(v))
    elif isinstance(value, tuple):
      # Decompresses and deserializes a single item.
      value = self._decompress_unserialize_value(value)
      self._store_value(entity, value)
    return value


class GenericProperty(Property):
  """A Property whose value can be (almost) any basic type.

  This is mainly used for Expando and for orphans (values present in
  the datastore but not represented in the Model subclass) but can
  also be used explicitly for properties with dynamically-typed
  values.
  """

  def _db_get_value(self, v, p):
    # This is awkward but there seems to be no faster way to inspect
    # what union member is present.  datastore_types.FromPropertyPb(),
    # the undisputed authority, has the same series of if-elif blocks.
    # (We don't even want to think about multiple members... :-)
    if v.has_stringvalue():
      sval = v.stringvalue()
      if p.meaning() not in (entity_pb.Property.BLOB,
                             entity_pb.Property.BYTESTRING):
        try:
          sval.decode('ascii')
          # If this passes, don't return unicode.
        except UnicodeDecodeError:
          try:
            sval = unicode(sval.decode('utf-8'))
          except UnicodeDecodeError:
            pass
      return sval
    elif v.has_int64value():
      ival = v.int64value()
      if p.meaning() == entity_pb.Property.GD_WHEN:
        return _EPOCH + datetime.timedelta(microseconds=ival)
      return ival
    elif v.has_booleanvalue():
      # The booleanvalue field is an int32, so booleanvalue() returns
      # an int, hence the conversion.
      return bool(v.booleanvalue())
    elif v.has_doublevalue():
      return v.doublevalue()
    elif v.has_referencevalue():
      rv = v.referencevalue()
      app = rv.app()
      namespace = rv.name_space()
      pairs = [(elem.type(), elem.id() or elem.name())
               for elem in rv.pathelement_list()]
      return Key(pairs=pairs, app=app, namespace=namespace)
    elif v.has_pointvalue():
      pv = v.pointvalue()
      return GeoPt(pv.x(), pv.y())
    elif v.has_uservalue():
      return _unpack_user(v)
    else:
      # A missing value implies null.
      return None

  def _db_set_value(self, v, p, value):
    # TODO: use a dict mapping types to functions
    if isinstance(value, str):
      v.set_stringvalue(value)
      # TODO: Set meaning to BLOB or BYTESTRING if it's not UTF-8?
      # (Or TEXT if unindexed.)
    elif isinstance(value, unicode):
      v.set_stringvalue(value.encode('utf8'))
      if not self._indexed:
        p.set_meaning(entity_pb.Property.TEXT)
    elif isinstance(value, bool):  # Must test before int!
      v.set_booleanvalue(value)
    elif isinstance(value, (int, long)):
      assert -2**63 <= value < 2**63
      v.set_int64value(value)
    elif isinstance(value, float):
      v.set_doublevalue(value)
    elif isinstance(value, Key):
      # See datastore_types.PackKey
      ref = value._reference()  # Don't copy
      rv = v.mutable_referencevalue()  # A Reference
      rv.set_app(ref.app())
      if ref.has_name_space():
        rv.set_name_space(ref.name_space())
      for elem in ref.path().element_list():
        rv.add_pathelement().CopyFrom(elem)
    elif isinstance(value, datetime.datetime):
      assert value.tzinfo is None
      dt = value - _EPOCH
      ival = dt.microseconds + 1000000 * (dt.seconds + 24*3600 * dt.days)
      v.set_int64value(ival)
      p.set_meaning(entity_pb.Property.GD_WHEN)
    elif isinstance(value, GeoPt):
      pv = v.mutable_pointvalue()
      pv.set_x(value.lat)
      pv.set_y(value.lon)
    elif isinstance(value, users.User):
      datastore_types.PackUser(p.name(), value, v)
    else:
      # TODO: BlobKey.
      assert False, type(value)


class ComputedProperty(GenericProperty):
  """A Property whose value is determined by a user-supplied function.

  Computed properties cannot be set directly, but are instead generated by a
  function when required. They are useful to provide fields in the datastore
  that can be used for filtering or sorting without having to manually set the
  value in code - for example, sorting on the length of a BlobProperty, or
  using an equality filter to check if another field is not empty.

  ComputedProperty can be declared as a regular property, passing a function as
  the first argument, or it can be used as a decorator for the function that
  does the calculation.

  Example:

  >>> class DatastoreFile(Model):
  ...   name = StringProperty()
  ...   name_lower = ComputedProperty(lambda self: self.name.lower())
  ...
  ...   data = BlobProperty()
  ...
  ...   @ComputedProperty
  ...   def size(self):
  ...     return len(self.data)
  ...
  ...   def _compute_hash(self):
  ...     return hashlib.sha1(self.data).hexdigest()
  ...   hash = ComputedProperty(_compute_hash, name='sha1')
  """

  def __init__(self, func, *args, **kwargs):
    """Constructor.

    Args:
      func: A function that takes one argument, the model instance, and returns
            a calculated value.
    """
    super(ComputedProperty, self).__init__(*args, **kwargs)
    assert not self._required, 'ComputedProperty cannot be required'
    assert not self._repeated, 'ComputedProperty cannot be repeated'
    assert self._default is None, 'ComputedProperty cannot have a default'
    self._func = func

  def _has_value(self, entity):
    return True

  def _store_value(self, entity, value):
    raise ComputedPropertyError("Cannot assign to a ComputedProperty")

  def _delete_value(self, entity):
    raise ComputedPropertyError("Cannot delete a ComputedProperty")

  def _retrieve_value(self, entity):
    return self._func(entity)

  def _deserialize(self, entity, p, depth=1):
    pass


class MetaModel(type):
  """Metaclass for Model.

  This exists to fix up the properties -- they need to know their name.
  This is accomplished by calling the class's _fix_properties() method.
  """

  def __init__(cls, name, bases, classdict):
    super(MetaModel, cls).__init__(name, bases, classdict)
    cls._fix_up_properties()


class Model(object):
  """A class describing datastore entities.

  Model instances are usually called entities.  All model classes
  inheriting from Model automatically have MetaModel as their
  metaclass, so that the properties are fixed up properly after the
  class once the class is defined.

  Because of this, you cannot use the same Property object to describe
  multiple properties -- you must create separate Property objects for
  each property.  E.g. this does not work:

    wrong_prop = StringProperty()
    class Wrong(Model):
      wrong1 = wrong_prop
      wrong2 = wrong_prop

  The kind is normally equal to the class name (exclusive of the
  module name or any other parent scope).  To override the kind,
  define a class method named _get_kind(), as follows:

    class MyModel(Model):
      @classmethod
      def _get_kind(cls):
        return 'AnotherKind'
  """

  __metaclass__ = MetaModel

  # Class variables updated by _fix_up_properties()
  _properties = None
  _has_repeated = False
  _kind_map = {}  # Dict mapping {kind: Model subclass}

  # Defaults for instance variables.
  _key = None
  _values = None

  # Hardcoded pseudo-property for the key.
  key = ModelKey()

  @datastore_rpc._positional(1)
  def __init__(self, key=None, id=None, parent=None, **kwds):
    """Creates a new instance of this model (a.k.a. as an entity).

    The new entity must be written to the datastore using an explicit
    call to .put().

    Args:
      key: Key instance for this model. If key is used, id and parent must
        be None.
      id: Key id for this model. If id is used, key must be None.
      parent: Key instance for the parent model or None for a top-level one.
        If parent is used, key must be None.
      **kwds: Keyword arguments mapping to properties of this model.

    Note: you cannot define a property named key; the .key attribute
    always refers to the entity's key.  But you can define properties
    named id or parent.  Values for the latter cannot be passed
    through the constructor, but can be assigned to entity attributes
    after the entity has been created.
    """
    if key is not None:
      if id is not None:
        raise datastore_errors.BadArgumentError(
            'Model constructor accepts key or id, not both.')
      if parent is not None:
        raise datastore_errors.BadArgumentError(
            'Model constructor accepts key or parent, not both.')
      self._key = _validate_key(key, entity=self)
    elif id is not None or parent is not None:
      # When parent is set but id is not, we have an incomplete key.
      # Key construction will fail with invalid ids or parents, so no check
      # is needed.
      # TODO: should this be restricted to string ids?
      self._key = Key(self._get_kind(), id, parent=parent)

    self._values = {}
    self._set_attributes(kwds)

  def _populate(self, **kwds):
    """Populate an instance from keyword arguments.

    Each keyword argument will be used to set a corresponding
    property.  Keywords must refer to valid property name.  This is
    similar to passing keyword arguments to the Model constructor,
    except that no provisions for key, id or parent are made.
    """
    self._set_attributes(kwds)
  populate = _populate

  def _set_attributes(self, kwds):
    """Internal helper to set attributes from keyword arguments.

    Expando overrides this.
    """
    cls = self.__class__
    for name, value in kwds.iteritems():
      prop = getattr(cls, name)  # Raises AttributeError for unknown properties.
      assert isinstance(prop, Property)
      prop._set_value(self, value)

  def _find_uninitialized(self):
    """Internal helper to find uninitialized properties.

    Returns:
      A set of property names.
    """
    return set(name
               for name, prop in self._properties.iteritems()
               if not prop._is_initialized(self))

  def _check_initialized(self):
    """Internal helper to check for uninitialized properties.

    Raises:
      BadValueError if it finds any.
    """
    baddies = self._find_uninitialized()
    if baddies:
      raise datastore_errors.BadValueError(
        'Entity has uninitialized properties: %s' % ', '.join(baddies))

  def __repr__(self):
    """Return an unambiguous string representation of an entity."""
    args = []
    done = set()
    for prop in self._properties.itervalues():
      if prop._has_value(self):
        args.append('%s=%r' % (prop._code_name, prop._retrieve_value(self)))
        done.add(prop._name)
    args.sort()
    if self._key is not None:
      args.insert(0, 'key=%r' % self._key)
    s = '%s(%s)' % (self.__class__.__name__, ', '.join(args))
    return s

  @classmethod
  def _get_kind(cls):
    """Return the kind name for this class.

    This defaults to cls.__name__; users may overrid this to give a
    class a different on-disk name than its class name.
    """
    return cls.__name__

  @classmethod
  def _get_kind_map(cls):
    """Internal helper to return the kind map."""
    return cls._kind_map

  @classmethod
  def _reset_kind_map(cls):
    """Clear the kind map.  Useful for testing."""
    cls._kind_map.clear()

  def _has_complete_key(self):
    """Return whether this entity has a complete key."""
    return self._key is not None and self._key.id() is not None

  def __hash__(self):
    """Dummy hash function.

    Raises:
      Always TypeError to emphasize that entities are mutable.
    """
    raise TypeError('Model is not immutable')

  def __eq__(self, other):
    """Compare two entities of the same class for equality."""
    if other.__class__ is not self.__class__:
      return NotImplemented
    # It's okay to use private names -- we're the same class
    if self._key != other._key:
      # TODO: If one key is None and the other is an explicit
      # incomplete key of the simplest form, this should be OK.
      return False
    return self._equivalent(other)

  def _equivalent(self, other):
    """Compare two entities of the same class, excluding keys."""
    assert other.__class__ is self.__class__  # TODO: What about subclasses?
    # It's all about determining inequality early.
    if len(self._properties) != len(other._properties):
      return False  # Can only happen for Expandos.
    my_prop_names = set(self._properties.iterkeys())
    their_prop_names = set(other._properties.iterkeys())
    if my_prop_names != their_prop_names:
      return False  # Again, only possible for Expandos.
    for name in my_prop_names:
      my_value = self._properties[name]._get_value(self)
      their_value = other._properties[name]._get_value(other)
      if my_value != their_value:
        return False
    return True

  def __ne__(self, other):
    """Implement self != other as not(self == other)."""
    eq = self.__eq__(other)
    if eq is NotImplemented:
      return NotImplemented
    return not eq

  def _to_pb(self, pb=None):
    """Internal helper to turn an entity into an EntityProto protobuf."""
    self._check_initialized()
    if pb is None:
      pb = entity_pb.EntityProto()

    # TODO: Move the key stuff into ModelAdapter.entity_to_pb()?
    key = self._key
    if key is None:
      pairs = [(self._get_kind(), None)]
      ref = ndb.key._ReferenceFromPairs(pairs, reference=pb.mutable_key())
    else:
      ref = key._reference()  # Don't copy
      pb.mutable_key().CopyFrom(ref)
    group = pb.mutable_entity_group()  # Must initialize this.
    # To work around an SDK issue, only set the entity group if the
    # full key is complete.  TODO: Remove the top test once fixed.
    if key is not None and key.id():
      elem = ref.path().element(0)
      if elem.id() or elem.name():
        group.add_element().CopyFrom(elem)

    for name, prop in sorted(self._properties.iteritems()):
      prop._serialize(self, pb)

    return pb

  @classmethod
  def _from_pb(cls, pb, set_key=True):
    """Internal helper to create an entity from an EntityProto protobuf."""
    assert isinstance(pb, entity_pb.EntityProto)
    ent = cls()

    # TODO: Move the key stuff into ModelAdapter.pb_to_entity()?
    if set_key and pb.has_key():
      ent._key = Key(reference=pb.key())

    indexed_properties = pb.property_list()
    unindexed_properties = pb.raw_property_list()
    for plist in [indexed_properties, unindexed_properties]:
      for p in plist:
        prop = ent._get_property_for(p, plist is indexed_properties)
        prop._deserialize(ent, p)

    return ent

  def _get_property_for(self, p, indexed=True, depth=0):
    """Internal helper to get the Property for a protobuf-level property."""
    name = p.name()
    parts = name.split('.')
    assert len(parts) > depth, (p.name(), parts, depth)
    next = parts[depth]
    prop = self._properties.get(next)
    if prop is None:
      prop = self._fake_property(p, next, indexed)
    return prop

  def _clone_properties(self):
    """Internal helper to clone self._properties if necessary."""
    cls = self.__class__
    if self._properties is cls._properties:
      self._properties = dict(cls._properties)

  def _fake_property(self, p, next, indexed=True):
    """Internal helper to create a fake Property."""
    self._clone_properties()
    if p.name() != next and not p.name().endswith('.' + next):
      prop = StructuredProperty(Expando, next)
      self._values[prop._name] = Expando()
    else:
      prop = GenericProperty(next,
                             repeated=p.multiple(),
                             indexed=indexed)
    self._properties[prop._name] = prop
    return prop

  @classmethod
  def _fix_up_properties(cls):
    """Fix up the properties by calling their _fix_up() method.

    Note: This is called by MetaModel, but may also be called manually
    after dynamically updating a model class.
    """
    cls._properties = {}  # Map of {name: Property}
    if cls.__module__ == __name__:  # Skip the classes in *this* file.
      return
    for name in set(dir(cls)):
      prop = getattr(cls, name, None)
      if isinstance(prop, ModelKey):
        continue
      if isinstance(prop, Property):
        assert not name.startswith('_')
        # TODO: Tell prop the class, for error message.
        prop._fix_up(name)
        if prop._repeated:
          cls._has_repeated = True
        cls._properties[prop._name] = prop
    cls._kind_map[cls._get_kind()] = cls

  # Datastore API using the default context.
  # These use local import since otherwise they'd be recursive imports.

  @classmethod
  def _query(cls, *args, **kwds):
    """Create a Query object for this class.

    Keyword arguments are passed to the Query() constructor.  If
    positional arguments are given they are used to apply an initial
    filter.

    Returns:
      A Query object.
    """
    from ndb.query import Query  # Import late to avoid circular imports.
    qry = Query(kind=cls._get_kind(), **kwds)
    if args:
      qry = qry.filter(*args)
    return qry
  query = _query

  def _put(self):
    """Write this entity to the datastore.

    If the operation creates or completes a key, the entity's key
    attribute is set to the new, complete key.

    Returns:
      The key for the entity.  This is always a complete key.
    """
    return self._put_async().get_result()
  put = _put

  def _put_async(self):
    """Write this entity to the datastore.

    This is the asynchronous version of Model._put().
    """
    from ndb import tasklets
    return tasklets.get_context().put(self)
  put_async = _put_async

  @classmethod
  def _get_or_insert(cls, name, parent=None, **kwds):
    """Transactionally retrieves an existing entity or creates a new one.

    Args:
      name: Key name to retrieve or create.
      parent: Parent entity key, if any.
      **kwds: Keyword arguments to pass to the constructor of the model class
        if an instance for the specified key name does not already exist. If
        an instance with the supplied key_name and parent already exists,
        these arguments will be discarded.

    Returns:
      Existing instance of Model class with the specified key name and parent
      or a new one that has just been created.
    """
    return cls._get_or_insert_async(name=name, parent=parent,
                                    **kwds).get_result()
  get_or_insert = _get_or_insert

  @classmethod
  def _get_or_insert_async(cls, name, parent=None, **kwds):
    """Transactionally retrieves an existing entity or creates a new one.

    This is the asynchronous version of Model._get_or_insert().
    """
    from ndb import tasklets
    ctx = tasklets.get_context()
    return ctx.get_or_insert(cls, name=name, parent=parent, **kwds)
  get_or_insert_async = _get_or_insert_async

  @classmethod
  def _allocate_ids(cls, size=None, max=None, parent=None):
    """Allocates a range of key IDs for this model class.

    Args:
      size: Number of IDs to allocate. Either size or max can be specified,
        not both.
      max: Maximum ID to allocate. Either size or max can be specified,
        not both.
      parent: Parent key for which the IDs will be allocated.

    Returns:
      A tuple with (start, end) for the allocated range, inclusive.
    """
    return cls._allocate_ids_async(size=size, max=max,
                                   parent=parent).get_result()
  allocate_ids = _allocate_ids

  @classmethod
  def _allocate_ids_async(cls, size=None, max=None, parent=None):
    """Allocates a range of key IDs for this model class.

    This is the asynchronous version of Model._allocate_ids().
    """
    from ndb import tasklets
    key = Key(cls._get_kind(), None, parent=parent)
    return tasklets.get_context().allocate_ids(key, size=size, max=max)
  allocate_ids_async = _allocate_ids_async

  @classmethod
  def _get_by_id(cls, id, parent=None):
    """Returns a instance of Model class by ID.

    Args:
      id: A string or integer key ID.
      parent: Parent key of the model to get.

    Returns:
      A model instance or None if not found.
    """
    return cls._get_by_id_async(id, parent=parent).get_result()
  get_by_id = _get_by_id

  @classmethod
  def _get_by_id_async(cls, id, parent=None):
    """Returns a instance of Model class by ID.

    This is the asynchronous version of Model._get_by_id().
    """
    from ndb import tasklets
    key = Key(cls._get_kind(), id, parent=parent)
    return tasklets.get_context().get(key)
  get_by_id_async = _get_by_id_async


class Expando(Model):
  """Model subclass to support dynamic Property names and types.

  See the module docstring for details.
  """

  def _set_attributes(self, kwds):
    for name, value in kwds.iteritems():
      setattr(self, name, value)

  def __getattr__(self, name):
    if name.startswith('_'):
      return super(Expando, self).__getattr__(name)
    prop = self._properties.get(name)
    if prop is None:
      return super(Expando, self).__getattribute__(name)
    return prop._get_value(self)

  def __setattr__(self, name, value):
    if (name.startswith('_') or
        isinstance(getattr(self.__class__, name, None), (Property, property))):
      return super(Expando, self).__setattr__(name, value)
    self._clone_properties()
    if isinstance(value, Model):
      prop = StructuredProperty(Model, name)
    else:
      prop = GenericProperty(name)
    prop._code_name = name
    self._properties[name] = prop
    prop._set_value(self, value)

  def __delattr__(self, name):
    if (name.startswith('_') or
        isinstance(getattr(self.__class__, name, None), (Property, property))):
      return super(Expando, self).__delattr__(name)
    prop = self._properties.get(name)
    assert prop is not None
    prop._delete_value(self)
    assert prop not in self.__class__._properties
    del self._properties[name]


@datastore_rpc._positional(1)
def transaction(callback, retry=None, entity_group=None):
  """Run a callback in a transaction.

  Args:
    callback: A function or tasklet to be called.
    retry: Optional retry count (keyword only; default set by
      ndb.context.Context.transaction()).
    entity_group: Optional root key to use as transaction entity group
      (keyword only; defaults to the root part of the first key used
      in the transaction).

  Returns:
    Whatever callback() returns.

  Raises:
    Whatever callback() raises; datastore_errors.TransactionFailedError
    if the transaction failed.

  Note:
    To pass arguments to a callback function, use a lambda, e.g.
      def my_callback(key, inc):
        ...
      transaction(lambda: my_callback(Key(...), 1))
  """
  fut = transaction_async(callback, retry=retry, entity_group=entity_group)
  return fut.get_result()


@datastore_rpc._positional(1)
def transaction_async(callback, retry=None, entity_group=None):
  """Run a callback in a transaction.

  This is the asynchronous version of transaction().
  """
  from ndb import tasklets
  kwds = {}
  if retry is not None:
    kwds['retry'] = retry
  if entity_group is not None:
    kwds['entity_group'] = entity_group
  return tasklets.get_context().transaction(callback, **kwds)


def in_transaction():
  """Return whether a transaction is currently active."""
  from ndb import tasklets
  return tasklets.get_context().in_transaction()


@datastore_rpc._positional(1)
def transactional(func):
  """Decorator to make a function automatically run in a transaction.

  If we're already in a transaction this is a no-op.

  Note: If you need to override the retry count or the entity group,
  or if you want some kind of async behavior, use the transaction()
  function above.
  """
  @utils.wrapping(func)
  def transactional_wrapper(*args, **kwds):
    if in_transaction():
      return func(*args, **kwds)
    else:
      return transaction(lambda: func(*args, **kwds))
  return transactional_wrapper


def get_multi_async(keys):
  """Fetches a sequence of keys.

  Args:
    keys: A sequence of keys.

  Returns:
    A list of futures.
  """
  return [key.get_async() for key in keys]


def get_multi(keys):
  """Fetches a sequence of keys.

  Args:
    keys: A sequence of keys.

  Returns:
    A list whose items are either a Model instance or None if the key wasn't
    found.
  """
  return [future.get_result() for future in get_multi_async(keys)]


def put_multi_async(models):
  """Stores a sequence of Model instances.

  Args:
    models: A sequence of Model instances.

  Returns:
    A list of futures.
  """
  return [model.put_async() for model in models]


def put_multi(models):
  """Stores a sequence of Model instances.

  Args:
    models: A sequence of Model instances.

  Returns:
    A list with the stored keys.
  """
  return [future.get_result() for future in put_multi_async(models)]


def delete_multi_async(keys):
  """Deletes a sequence of keys.

  Returns:
    A list of futures.
  """
  return [key.delete_async() for key in keys]


def delete_multi(keys):
  """Deletes a sequence of keys.

  Args:
    keys: A sequence of keys.
  """
  # A list full of Nones!!!
  return [future.get_result() for future in delete_multi_async(keys)]


# Update __all__ to contain all Property and Exception subclasses.
for _name, _object in globals().items():
  if ((_name.endswith('Property') and issubclass(_object, Property)) or
      (_name.endswith('Error') and issubclass(_object, Exception))):
    __all__.append(_name)
