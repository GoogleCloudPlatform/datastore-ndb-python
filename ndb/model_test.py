"""Tests for model.py."""

import base64
import datetime
import difflib
import pickle
import re
import unittest

from google.appengine.api import datastore_errors
from google.appengine.api import datastore_types
from google.appengine.api import namespace_manager
from google.appengine.api import users
from google.appengine.datastore import entity_pb

from ndb import model, query, tasklets, test_utils

TESTUSER = users.User('test@example.com', 'example.com', '123')
AMSTERDAM = model.GeoPt(52.35, 4.9166667)

GOLDEN_PB = """\
key <
  app: "_"
  path <
    Element {
      type: "Model"
      id: 42
    }
  >
>
entity_group <
  Element {
    type: "Model"
    id: 42
  }
>
property <
  name: "b"
  value <
    booleanValue: true
  >
  multiple: false
>
property <
  name: "d"
  value <
    doubleValue: 2.5
  >
  multiple: false
>
property <
  name: "k"
  value <
    ReferenceValue {
      app: "_"
      PathElement {
        type: "Model"
        id: 42
      }
    }
  >
  multiple: false
>
property <
  name: "p"
  value <
    int64Value: 42
  >
  multiple: false
>
property <
  name: "q"
  value <
    stringValue: "hello"
  >
  multiple: false
>
property <
  name: "u"
  value <
    UserValue {
      email: "test@example.com"
      auth_domain: "example.com"
      gaiaid: 0
      obfuscated_gaiaid: "123"
    }
  >
  multiple: false
>
property <
  name: "xy"
  value <
    PointValue {
      x: 52.35
      y: 4.9166667
    }
  >
  multiple: false
>
"""

INDEXED_PB = re.sub('Model', 'MyModel', GOLDEN_PB)

UNINDEXED_PB = """\
key <
  app: "_"
  path <
    Element {
      type: "MyModel"
      id: 0
    }
  >
>
entity_group <
>
raw_property <
  meaning: 14
  name: "b"
  value <
    stringValue: "\\000\\377"
  >
  multiple: false
>
raw_property <
  meaning: 15
  name: "t"
  value <
    stringValue: "Hello world\\341\\210\\264"
  >
  multiple: false
>
"""

PERSON_PB = """\
key <
  app: "_"
  path <
    Element {
      type: "Person"
      id: 0
    }
  >
>
entity_group <
>
property <
  name: "address.city"
  value <
    stringValue: "Mountain View"
  >
  multiple: false
>
property <
  name: "address.street"
  value <
    stringValue: "1600 Amphitheatre"
  >
  multiple: false
>
property <
  name: "name"
  value <
    stringValue: "Google"
  >
  multiple: false
>
"""

NESTED_PB = """\
key <
  app: "_"
  path <
    Element {
      type: "Person"
      id: 0
    }
  >
>
entity_group <
>
property <
  name: "address.home.city"
  value <
    stringValue: "Mountain View"
  >
  multiple: false
>
property <
  name: "address.home.street"
  value <
    stringValue: "1600 Amphitheatre"
  >
  multiple: false
>
property <
  name: "address.work.city"
  value <
    stringValue: "San Francisco"
  >
  multiple: false
>
property <
  name: "address.work.street"
  value <
    stringValue: "345 Spear"
  >
  multiple: false
>
property <
  name: "name"
  value <
    stringValue: "Google"
  >
  multiple: false
>
"""

RECURSIVE_PB = """\
key <
  app: "_"
  path <
    Element {
      type: "Tree"
      id: 0
    }
  >
>
entity_group <
>
raw_property <
  meaning: 15
  name: "root.left.left.name"
  value <
    stringValue: "a1a"
  >
  multiple: false
>
raw_property <
  meaning: 15
  name: "root.left.name"
  value <
    stringValue: "a1"
  >
  multiple: false
>
raw_property <
  meaning: 15
  name: "root.left.rite.name"
  value <
    stringValue: "a1b"
  >
  multiple: false
>
raw_property <
  meaning: 15
  name: "root.name"
  value <
    stringValue: "a"
  >
  multiple: false
>
raw_property <
  meaning: 15
  name: "root.rite.name"
  value <
    stringValue: "a2"
  >
  multiple: false
>
raw_property <
  meaning: 15
  name: "root.rite.rite.name"
  value <
    stringValue: "a2b"
  >
  multiple: false
>
"""

MULTI_PB = """\
key <
  app: "_"
  path <
    Element {
      type: "Person"
      id: 0
    }
  >
>
entity_group <
>
property <
  name: "address"
  value <
    stringValue: "345 Spear"
  >
  multiple: true
>
property <
  name: "address"
  value <
    stringValue: "San Francisco"
  >
  multiple: true
>
property <
  name: "name"
  value <
    stringValue: "Google"
  >
  multiple: false
>
"""

MULTIINSTRUCT_PB = """\
key <
  app: "_"
  path <
    Element {
      type: "Person"
      id: 0
    }
  >
>
entity_group <
>
property <
  name: "address.label"
  value <
    stringValue: "work"
  >
  multiple: false
>
property <
  name: "address.line"
  value <
    stringValue: "345 Spear"
  >
  multiple: true
>
property <
  name: "address.line"
  value <
    stringValue: "San Francisco"
  >
  multiple: true
>
property <
  name: "name"
  value <
    stringValue: "Google"
  >
  multiple: false
>
"""

MULTISTRUCT_PB = """\
key <
  app: "_"
  path <
    Element {
      type: "Person"
      id: 0
    }
  >
>
entity_group <
>
property <
  name: "address.label"
  value <
    stringValue: "work"
  >
  multiple: true
>
property <
  name: "address.text"
  value <
    stringValue: "San Francisco"
  >
  multiple: true
>
property <
  name: "address.label"
  value <
    stringValue: "home"
  >
  multiple: true
>
property <
  name: "address.text"
  value <
    stringValue: "Mountain View"
  >
  multiple: true
>
property <
  name: "name"
  value <
    stringValue: "Google"
  >
  multiple: false
>
"""

class ModelTests(test_utils.DatastoreTest):

  def tearDown(self):
    self.assertTrue(model.Model._properties == {})
    self.assertTrue(model.Expando._properties == {})
    super(ModelTests, self).tearDown()

  def testKey(self):
    m = model.Model()
    self.assertEqual(m.key, None)
    k = model.Key(flat=['ParentModel', 42, 'Model', 'foobar'])
    m.key = k
    self.assertEqual(m.key, k)
    del m.key
    self.assertEqual(m.key, None)
    # incomplete key
    k2 = model.Key(flat=['ParentModel', 42, 'Model', None])
    m.key = k2
    self.assertEqual(m.key, k2)

  def testIncompleteKey(self):
    m = model.Model()
    k = model.Key(flat=['Model', None])
    m.key = k
    pb = m._to_pb()
    m2 = model.Model._from_pb(pb)
    self.assertEqual(m2, m)

  def testIdAndParent(self):
    p = model.Key('ParentModel', 'foo')

    # key name
    m = model.Model(id='bar')
    m2 = model.Model._from_pb(m._to_pb())
    self.assertEqual(m2.key, model.Key('Model', 'bar'))

    # key name + parent
    m = model.Model(id='bar', parent=p)
    m2 = model.Model._from_pb(m._to_pb())
    self.assertEqual(m2.key, model.Key('ParentModel', 'foo', 'Model', 'bar'))

    # key id
    m = model.Model(id=42)
    m2 = model.Model._from_pb(m._to_pb())
    self.assertEqual(m2.key, model.Key('Model', 42))

    # key id + parent
    m = model.Model(id=42, parent=p)
    m2 = model.Model._from_pb(m._to_pb())
    self.assertEqual(m2.key, model.Key('ParentModel', 'foo', 'Model', 42))

    # parent
    m = model.Model(parent=p)
    m2 = model.Model._from_pb(m._to_pb())
    self.assertEqual(m2.key, model.Key('ParentModel', 'foo', 'Model', None))

    # not key -- invalid
    self.assertRaises(datastore_errors.BadValueError, model.Model, key='foo')

    # wrong key kind -- invalid
    k = model.Key('OtherModel', 'bar')
    class MyModel(model.Model):
      pass
    self.assertRaises(model.KindError, MyModel, key=k)

    # incomplete parent -- invalid
    p2 = model.Key('ParentModel', None)
    self.assertRaises(datastore_errors.BadArgumentError, model.Model,
                      parent=p2)
    self.assertRaises(datastore_errors.BadArgumentError, model.Model,
                      id='bar', parent=p2)

    # key + id -- invalid
    k = model.Key('Model', 'bar')
    self.assertRaises(datastore_errors.BadArgumentError, model.Model, key=k,
                      id='bar')

    # key + parent -- invalid
    k = model.Key('Model', 'bar', parent=p)
    self.assertRaises(datastore_errors.BadArgumentError, model.Model, key=k,
                      parent=p)

    # key + id + parent -- invalid
    self.assertRaises(datastore_errors.BadArgumentError, model.Model, key=k,
                      id='bar', parent=p)

  def testQuery(self):
    class MyModel(model.Model):
      p = model.IntegerProperty()

    q = MyModel.query()
    self.assertTrue(isinstance(q, query.Query))
    self.assertEqual(q.kind, 'MyModel')
    self.assertEqual(q.ancestor, None)

    k = model.Key(flat=['Model', 1])
    q = MyModel.query(ancestor=k)
    self.assertEqual(q.kind, 'MyModel')
    self.assertEqual(q.ancestor, k)

    k0 = model.Key(flat=['Model', None])
    self.assertRaises(Exception, MyModel.query, ancestor=k0)

  def testQueryWithFilter(self):
    class MyModel(model.Model):
      p = model.IntegerProperty()

    q = MyModel.query(MyModel.p >= 0)
    self.assertTrue(isinstance(q, query.Query))
    self.assertEqual(q.kind, 'MyModel')
    self.assertEqual(q.ancestor, None)
    self.assertTrue(q.filters is not None)

    q2 = MyModel.query().filter(MyModel.p >= 0)
    self.assertEqual(q.filters, q2.filters)

  def testProperty(self):
    class MyModel(model.Model):
      b = model.BooleanProperty()
      p = model.IntegerProperty()
      q = model.StringProperty()
      d = model.FloatProperty()
      k = model.KeyProperty()
      u = model.UserProperty()
      xy = model.GeoPtProperty()

    ent = MyModel()
    k = model.Key(flat=['MyModel', 42])
    ent.key = k
    MyModel.b._set_value(ent, True)
    MyModel.p._set_value(ent, 42)
    MyModel.q._set_value(ent, 'hello')
    MyModel.d._set_value(ent, 2.5)
    MyModel.k._set_value(ent, k)
    MyModel.u._set_value(ent, TESTUSER)
    MyModel.xy._set_value(ent, AMSTERDAM)
    self.assertEqual(MyModel.b._get_value(ent), True)
    self.assertEqual(MyModel.p._get_value(ent), 42)
    self.assertEqual(MyModel.q._get_value(ent), 'hello')
    self.assertEqual(MyModel.d._get_value(ent), 2.5)
    self.assertEqual(MyModel.k._get_value(ent), k)
    self.assertEqual(MyModel.u._get_value(ent), TESTUSER)
    self.assertEqual(MyModel.xy._get_value(ent), AMSTERDAM)
    pb = self.conn.adapter.entity_to_pb(ent)
    self.assertEqual(str(pb), INDEXED_PB)

    ent = MyModel._from_pb(pb)
    self.assertEqual(ent._get_kind(), 'MyModel')
    k = model.Key(flat=['MyModel', 42])
    self.assertEqual(ent.key, k)
    self.assertEqual(MyModel.p._get_value(ent), 42)
    self.assertEqual(MyModel.q._get_value(ent), 'hello')
    self.assertEqual(MyModel.d._get_value(ent), 2.5)
    self.assertEqual(MyModel.k._get_value(ent), k)

  def testDeletingPropertyValue(self):
    class MyModel(model.Model):
      a = model.StringProperty()
    m = MyModel()

    # Initially it isn't there (but the value defaults to None).
    self.assertEqual(m.a, None)
    self.assertFalse(MyModel.a._has_value(m))

    # Explicit None assignment makes it present.
    m.a = None
    self.assertEqual(m.a, None)
    self.assertTrue(MyModel.a._has_value(m))

    # Deletion restores the initial state.
    del m.a
    self.assertEqual(m.a, None)
    self.assertFalse(MyModel.a._has_value(m))

    # Redundant deletions are okay.
    del m.a
    self.assertEqual(m.a, None)
    self.assertFalse(MyModel.a._has_value(m))

    # Deleted/missing values are serialized and considered present
    # when deserialized.
    pb = m._to_pb()
    m = MyModel._from_pb(pb)
    self.assertEqual(m.a, None)
    self.assertTrue(MyModel.a._has_value(m))

  def testDefaultPropertyValue(self):
    class MyModel(model.Model):
      a = model.StringProperty(default='a')
      b = model.StringProperty(default='')
    m = MyModel()

    # Initial values equal the defaults.
    self.assertEqual(m.a, 'a')
    self.assertEqual(m.b, '')
    self.assertFalse(MyModel.a._has_value(m))
    self.assertFalse(MyModel.b._has_value(m))

    # Setting values erases the defaults.
    m.a = ''
    m.b = 'b'
    self.assertEqual(m.a, '')
    self.assertEqual(m.b, 'b')
    self.assertTrue(MyModel.a._has_value(m))
    self.assertTrue(MyModel.b._has_value(m))

    # Deleting values restores the defaults.
    del m.a
    del m.b
    self.assertEqual(m.a, 'a')
    self.assertEqual(m.b, '')
    self.assertFalse(MyModel.a._has_value(m))
    self.assertFalse(MyModel.b._has_value(m))

    # Serialization makes the default values explicit.
    pb = m._to_pb()
    m = MyModel._from_pb(pb)
    self.assertEqual(m.a, 'a')
    self.assertEqual(m.b, '')
    self.assertTrue(MyModel.a._has_value(m))
    self.assertTrue(MyModel.b._has_value(m))

  def testComparingExplicitAndImplicitValue(self):
    class MyModel(model.Model):
      a = model.StringProperty(default='a')
      b = model.StringProperty()
    m1 = MyModel(b=None)
    m2 = MyModel()
    self.assertEqual(m1, m2)
    m1.a = 'a'
    self.assertEqual(m1, m2)

  def testRequiredProperty(self):
    class MyModel(model.Model):
      a = model.StringProperty(required=True)
      b = model.StringProperty()  # Never counts as uninitialized
    self.assertEqual(repr(MyModel.a), "StringProperty('a', required=True)")
    m = MyModel()

    # Never-assigned values are considered uninitialized.
    self.assertEqual(m._find_uninitialized(), set(['a']))
    self.assertRaises(datastore_errors.BadValueError, m._check_initialized)
    self.assertRaises(datastore_errors.BadValueError, m._to_pb)

    # Empty string is fine.
    m.a = ''
    self.assertFalse(m._find_uninitialized())
    m._check_initialized()
    m._to_pb()

    # Non-empty string is fine (of course).
    m.a = 'foo'
    self.assertFalse(m._find_uninitialized())
    m._check_initialized()
    m._to_pb()

    # Deleted value is not fine.
    del m.a
    self.assertEqual(m._find_uninitialized(), set(['a']))
    self.assertRaises(datastore_errors.BadValueError, m._check_initialized)
    self.assertRaises(datastore_errors.BadValueError, m._to_pb)

    # Explicitly assigned None is *not* fine.
    m.a = None
    self.assertEqual(m._find_uninitialized(), set(['a']))
    self.assertRaises(datastore_errors.BadValueError, m._check_initialized)
    self.assertRaises(datastore_errors.BadValueError, m._to_pb)

    # Check that b is still unset.
    self.assertFalse(MyModel.b._has_value(m))

  def testRepeatedRequiredDefaultConflict(self):
    # Allow at most one of repeated=True, required=True, default=<non-None>.
    class MyModel(model.Model):
      self.assertRaises(Exception,
                        model.StringProperty, repeated=True, default='')
      self.assertRaises(Exception,
                        model.StringProperty, repeated=True, required=True)
      self.assertRaises(Exception,
                        model.StringProperty, required=True, default='')
      self.assertRaises(Exception,
                        model.StringProperty,
                        repeated=True, required=True, default='')

  def testBlobKeyProperty(self):
    class MyModel(model.Model):
      image = model.BlobKeyProperty()
    test_blobkey = datastore_types.BlobKey('testkey123')
    m = MyModel()
    m.image = test_blobkey
    m.put()

    m = m.key.get()

    self.assertTrue(isinstance(m.image, datastore_types.BlobKey))
    self.assertEqual(str(m.image), str(test_blobkey))

  def testChoicesProperty(self):
    class MyModel(model.Model):
      a = model.StringProperty(choices=['a', 'b', 'c'])
      b = model.IntegerProperty(choices=[1, 2, 3], repeated=True)
    m = MyModel(a='a', b=[1, 2])
    m.a = 'b'
    m.a = None
    m.b = [1, 1, 3]
    m.b = []
    self.assertRaises(datastore_errors.BadValueError,
                      setattr, m, 'a', 'A')
    self.assertRaises(datastore_errors.BadValueError,
                      setattr, m, 'b', [42])

  def testValidatorProperty(self):
    def my_validator(prop, value):
      value = value.lower()
      if not value.startswith('a'):
        raise datastore_errors.BadValueError('%s does not start with "a"' %
                                             prop._name)
      return value
    class MyModel(model.Model):
      a = model.StringProperty(validator=my_validator)
    m = MyModel()
    m.a = 'ABC'
    self.assertEqual(m.a, 'abc')
    self.assertRaises(datastore_errors.BadValueError,
                      setattr, m, 'a', 'def')

  def testUnindexedProperty(self):
    class MyModel(model.Model):
      t = model.TextProperty()
      b = model.BlobProperty()

    ent = MyModel()
    MyModel.t._set_value(ent, u'Hello world\u1234')
    MyModel.b._set_value(ent, '\x00\xff')
    self.assertEqual(MyModel.t._get_value(ent), u'Hello world\u1234')
    self.assertEqual(MyModel.b._get_value(ent), '\x00\xff')
    pb = ent._to_pb()
    self.assertEqual(str(pb), UNINDEXED_PB)

    ent = MyModel._from_pb(pb)
    self.assertEqual(ent._get_kind(), 'MyModel')
    k = model.Key(flat=['MyModel', None])
    self.assertEqual(ent.key, k)
    self.assertEqual(MyModel.t._get_value(ent), u'Hello world\u1234')
    self.assertEqual(MyModel.b._get_value(ent), '\x00\xff')

  def DateAndOrTimePropertyTest(self, propclass, t1, t2):
    class Person(model.Model):
      name = model.StringProperty()
      ctime = propclass(auto_now_add=True)
      mtime = propclass(auto_now=True)
      atime = propclass()
      times =  propclass(repeated=True)

    p = Person()
    p.atime = t1
    p.times = [t1, t2]
    self.assertEqual(p.ctime, None)
    self.assertEqual(p.mtime, None)
    pb = p._to_pb()
    self.assertNotEqual(p.ctime, None)
    self.assertNotEqual(p.mtime, None)
    q = Person._from_pb(pb)
    self.assertEqual(q.ctime, p.ctime)
    self.assertEqual(q.mtime, p.mtime)
    self.assertEqual(q.atime, t1)
    self.assertEqual(q.times, [t1, t2])

  def testDateTimeProperty(self):
    self.DateAndOrTimePropertyTest(model.DateTimeProperty,
                                   datetime.datetime(1982, 12, 1, 9, 0, 0),
                                   datetime.datetime(1995, 4, 15, 5, 0, 0))

  def testDateProperty(self):
    self.DateAndOrTimePropertyTest(model.DateProperty,
                                   datetime.date(1982, 12, 1),
                                   datetime.date(1995, 4, 15))

  def testTimeProperty(self):
    self.DateAndOrTimePropertyTest(model.TimeProperty,
                                   datetime.time(9, 0, 0),
                                   datetime.time(5, 0, 0, 500))

  def testStructuredProperty(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()
    class Person(model.Model):
      name = model.StringProperty()
      address = model.StructuredProperty(Address)

    p = Person()
    p.name = 'Google'
    a = Address(street='1600 Amphitheatre')
    p.address = a
    p.address.city = 'Mountain View'
    self.assertEqual(Person.name._get_value(p), 'Google')
    self.assertEqual(p.name, 'Google')
    self.assertEqual(Person.address._get_value(p), a)
    self.assertEqual(Address.street._get_value(a), '1600 Amphitheatre')
    self.assertEqual(Address.city._get_value(a), 'Mountain View')

    pb = p._to_pb()
    self.assertEqual(str(pb), PERSON_PB)

    p = Person._from_pb(pb)
    self.assertEqual(p.name, 'Google')
    self.assertEqual(p.address.street, '1600 Amphitheatre')
    self.assertEqual(p.address.city, 'Mountain View')
    self.assertEqual(p.address, a)

  def testNestedStructuredProperty(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()
    class AddressPair(model.Model):
      home = model.StructuredProperty(Address)
      work = model.StructuredProperty(Address)
    class Person(model.Model):
      name = model.StringProperty()
      address = model.StructuredProperty(AddressPair)

    p = Person()
    p.name = 'Google'
    p.address = AddressPair(home=Address(), work=Address())
    p.address.home.city = 'Mountain View'
    p.address.home.street = '1600 Amphitheatre'
    p.address.work.city = 'San Francisco'
    p.address.work.street = '345 Spear'
    pb = p._to_pb()
    self.assertEqual(str(pb), NESTED_PB)

    p = Person._from_pb(pb)
    self.assertEqual(p.name, 'Google')
    self.assertEqual(p.address.home.street, '1600 Amphitheatre')
    self.assertEqual(p.address.home.city, 'Mountain View')
    self.assertEqual(p.address.work.street, '345 Spear')
    self.assertEqual(p.address.work.city, 'San Francisco')

  def testRecursiveStructuredProperty(self):
    class Node(model.Model):
      name = model.StringProperty(indexed=False)
    Node.left = model.StructuredProperty(Node)
    Node.rite = model.StructuredProperty(Node)
    Node._fix_up_properties()
    class Tree(model.Model):
      root = model.StructuredProperty(Node)

    k = model.Key(flat=['Tree', None])
    tree = Tree()
    tree.key = k
    tree.root = Node(name='a',
                     left=Node(name='a1',
                               left=Node(name='a1a'),
                               rite=Node(name='a1b')),
                     rite=Node(name='a2',
                               rite=Node(name='a2b')))
    pb = tree._to_pb()
    self.assertEqual(str(pb), RECURSIVE_PB)

    tree2 = Tree._from_pb(pb)
    self.assertEqual(tree2, tree)

  def testRenamedProperty(self):
    class MyModel(model.Model):
      bb = model.BooleanProperty('b')
      pp = model.IntegerProperty('p')
      qq = model.StringProperty('q')
      dd = model.FloatProperty('d')
      kk = model.KeyProperty('k')
      uu = model.UserProperty('u')
      xxyy = model.GeoPtProperty('xy')

    ent = MyModel()
    k = model.Key(flat=['MyModel', 42])
    ent.key = k
    MyModel.bb._set_value(ent, True)
    MyModel.pp._set_value(ent, 42)
    MyModel.qq._set_value(ent, 'hello')
    MyModel.dd._set_value(ent, 2.5)
    MyModel.kk._set_value(ent, k)
    MyModel.uu._set_value(ent, TESTUSER)
    MyModel.xxyy._set_value(ent, AMSTERDAM)
    self.assertEqual(MyModel.pp._get_value(ent), 42)
    self.assertEqual(MyModel.qq._get_value(ent), 'hello')
    self.assertEqual(MyModel.dd._get_value(ent), 2.5)
    self.assertEqual(MyModel.kk._get_value(ent), k)
    self.assertEqual(MyModel.uu._get_value(ent), TESTUSER)
    self.assertEqual(MyModel.xxyy._get_value(ent), AMSTERDAM)
    pb = self.conn.adapter.entity_to_pb(ent)
    self.assertEqual(str(pb), INDEXED_PB)

    ent = MyModel._from_pb(pb)
    self.assertEqual(ent._get_kind(), 'MyModel')
    k = model.Key(flat=['MyModel', 42])
    self.assertEqual(ent.key, k)
    self.assertEqual(MyModel.pp._get_value(ent), 42)
    self.assertEqual(MyModel.qq._get_value(ent), 'hello')
    self.assertEqual(MyModel.dd._get_value(ent), 2.5)
    self.assertEqual(MyModel.kk._get_value(ent), k)

  def testRenamedStructuredProperty(self):
    class Address(model.Model):
      st = model.StringProperty('street')
      ci = model.StringProperty('city')
    class AddressPair(model.Model):
      ho = model.StructuredProperty(Address, 'home')
      wo = model.StructuredProperty(Address, 'work')
    class Person(model.Model):
      na = model.StringProperty('name')
      ad = model.StructuredProperty(AddressPair, 'address')

    p = Person()
    p.na = 'Google'
    p.ad = AddressPair(ho=Address(), wo=Address())
    p.ad.ho.ci = 'Mountain View'
    p.ad.ho.st = '1600 Amphitheatre'
    p.ad.wo.ci = 'San Francisco'
    p.ad.wo.st = '345 Spear'
    pb = p._to_pb()
    self.assertEqual(str(pb), NESTED_PB)

    p = Person._from_pb(pb)
    self.assertEqual(p.na, 'Google')
    self.assertEqual(p.ad.ho.st, '1600 Amphitheatre')
    self.assertEqual(p.ad.ho.ci, 'Mountain View')
    self.assertEqual(p.ad.wo.st, '345 Spear')
    self.assertEqual(p.ad.wo.ci, 'San Francisco')

  def testKindMap(self):
    model.Model._reset_kind_map()
    class A1(model.Model):
      pass
    self.assertEqual(model.Model._get_kind_map(), {'A1': A1})
    class A2(model.Model):
      pass
    self.assertEqual(model.Model._get_kind_map(), {'A1': A1, 'A2': A2})

  def testMultipleProperty(self):
    class Person(model.Model):
      name = model.StringProperty()
      address = model.StringProperty(repeated=True)

    m = Person(name='Google', address=['345 Spear', 'San Francisco'])
    m.key = model.Key(flat=['Person', None])
    self.assertEqual(m.address, ['345 Spear', 'San Francisco'])
    pb = m._to_pb()
    self.assertEqual(str(pb), MULTI_PB)

    m2 = Person._from_pb(pb)
    self.assertEqual(m2, m)

  def testMultipleInStructuredProperty(self):
    class Address(model.Model):
      label = model.StringProperty()
      line = model.StringProperty(repeated=True)
    class Person(model.Model):
      name = model.StringProperty()
      address = model.StructuredProperty(Address)

    m = Person(name='Google',
               address=Address(label='work',
                               line=['345 Spear', 'San Francisco']))
    m.key = model.Key(flat=['Person', None])
    self.assertEqual(m.address.line, ['345 Spear', 'San Francisco'])
    pb = m._to_pb()
    self.assertEqual(str(pb), MULTIINSTRUCT_PB)

    m2 = Person._from_pb(pb)
    self.assertEqual(m2, m)

  def testMultipleStructuredProperty(self):
    class Address(model.Model):
      label = model.StringProperty()
      text = model.StringProperty()
    class Person(model.Model):
      name = model.StringProperty()
      address = model.StructuredProperty(Address, repeated=True)

    m = Person(name='Google',
               address=[Address(label='work', text='San Francisco'),
                        Address(label='home', text='Mountain View')])
    m.key = model.Key(flat=['Person', None])
    self.assertEqual(m.address[0].label, 'work')
    self.assertEqual(m.address[0].text, 'San Francisco')
    self.assertEqual(m.address[1].label, 'home')
    self.assertEqual(m.address[1].text, 'Mountain View')
    pb = m._to_pb()
    self.assertEqual(str(pb), MULTISTRUCT_PB)

    m2 = Person._from_pb(pb)
    self.assertEqual(m2, m)

  def testCannotMultipleInMultiple(self):
    class Inner(model.Model):
      innerval = model.StringProperty(repeated=True)
    self.assertRaises(AssertionError,
                      model.StructuredProperty, Inner, repeated=True)

  def testNullProperties(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()
      zip = model.IntegerProperty()
    class Person(model.Model):
      address = model.StructuredProperty(Address)
      age = model.IntegerProperty()
      name = model.StringProperty()
      k = model.KeyProperty()
    k = model.Key(flat=['Person', 42])
    p = Person()
    p.key = k
    self.assertEqual(p.address, None)
    self.assertEqual(p.age, None)
    self.assertEqual(p.name, None)
    self.assertEqual(p.k, None)
    pb = p._to_pb()
    q = Person._from_pb(pb)
    self.assertEqual(q.address, None)
    self.assertEqual(q.age, None)
    self.assertEqual(q.name, None)
    self.assertEqual(q.k, None)
    self.assertEqual(q, p)

  def testOrphanProperties(self):
    class Tag(model.Model):
      names = model.StringProperty(repeated=True)
      ratings = model.IntegerProperty(repeated=True)
    class Address(model.Model):
      line = model.StringProperty(repeated=True)
      city = model.StringProperty()
      zip = model.IntegerProperty()
      tags = model.StructuredProperty(Tag)
    class Person(model.Model):
      address = model.StructuredProperty(Address)
      age = model.IntegerProperty(repeated=True)
      name = model.StringProperty()
      k = model.KeyProperty()
    k = model.Key(flat=['Person', 42])
    p = Person(name='White House', k=k, age=[210, 211],
               address=Address(line=['1600 Pennsylvania', 'Washington, DC'],
                               tags=Tag(names=['a', 'b'], ratings=[1, 2]),
                               zip=20500))
    p.key = k
    pb = p._to_pb()
    q = model.Model._from_pb(pb)
    qb = q._to_pb()
    linesp = str(pb).splitlines(True)
    linesq = str(qb).splitlines(True)
    lines = difflib.unified_diff(linesp, linesq, 'Expected', 'Actual')
    self.assertEqual(pb, qb, ''.join(lines))

  def testModelRepr(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()
    class Person(model.Model):
      name = model.StringProperty()
      address = model.StructuredProperty(Address)

    p = Person(name='Google', address=Address(street='345 Spear', city='SF'))
    self.assertEqual(
      repr(p),
      "Person(address=Address(city='SF', street='345 Spear'), name='Google')")
    p.key = model.Key(pairs=[('Person', 42)])
    self.assertEqual(
      repr(p),
      "Person(key=Key('Person', 42), "
      "address=Address(city='SF', street='345 Spear'), name='Google')")

  def testModelRepr_RenamedProperty(self):
    class Address(model.Model):
      street = model.StringProperty('Street')
      city = model.StringProperty('City')
    a = Address(street='345 Spear', city='SF')
    self.assertEqual(repr(a), "Address(city='SF', street='345 Spear')")

  def testModel_RenameAlias(self):
    class Person(model.Model):
      name = model.StringProperty('Name')
    p = Person(name='Fred')
    self.assertRaises(AttributeError, getattr, p, 'Name')
    self.assertRaises(AttributeError, Person, Name='Fred')
    # Unfortunately, p.Name = 'boo' just sets p.__dict__['Name'] = 'boo'.
    self.assertRaises(AttributeError, getattr, p, 'foo')

  def testExpando_RenameAlias(self):
    class Person(model.Expando):
      name = model.StringProperty('Name')

    p = Person(name='Fred')
    self.assertEqual(p.name, 'Fred')
    self.assertEqual(p.Name, 'Fred')
    self.assertEqual(p._values, {'Name': 'Fred'})
    self.assertTrue(p._properties, Person._properties)

    p = Person(Name='Fred')
    self.assertEqual(p.name, 'Fred')
    self.assertEqual(p.Name, 'Fred')
    self.assertEqual(p._values, {'Name': 'Fred'})
    self.assertTrue(p._properties, Person._properties)

    p = Person()
    p.Name = 'Fred'
    self.assertEqual(p.name, 'Fred')
    self.assertEqual(p.Name, 'Fred')
    self.assertEqual(p._values, {'Name': 'Fred'})
    self.assertTrue(p._properties, Person._properties)

    self.assertRaises(AttributeError, getattr, p, 'foo')

  def testModel_RenameSwap(self):
    class Person(model.Model):
      foo = model.StringProperty('bar')
      bar = model.StringProperty('foo')
    p = Person(foo='foo', bar='bar')
    self.assertEqual(p._values,
                     {'foo': 'bar', 'bar': 'foo'})

  def testExpando_RenameSwap(self):
    class Person(model.Expando):
      foo = model.StringProperty('bar')
      bar = model.StringProperty('foo')
    p = Person(foo='foo', bar='bar', baz='baz')
    self.assertEqual(p._values,
                     {'foo': 'bar', 'bar': 'foo', 'baz': 'baz'})
    p = Person()
    p.foo = 'foo'
    p.bar = 'bar'
    p.baz = 'baz'
    self.assertEqual(p._values,
                     {'foo': 'bar', 'bar': 'foo', 'baz': 'baz'})

  def testPropertyRepr(self):
    p = model.Property()
    self.assertEqual(repr(p), 'Property()')
    p = model.IntegerProperty('foo', indexed=False, repeated=True)
    self.assertEqual(repr(p),
                     "IntegerProperty('foo', indexed=False, repeated=True)")
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()
    p = model.StructuredProperty(Address, 'foo')
    self.assertEqual(repr(p), "StructuredProperty(Address, 'foo')")

  def testValidation(self):
    class All(model.Model):
      s = model.StringProperty()
      i = model.IntegerProperty()
      f = model.FloatProperty()
      t = model.TextProperty()
      b = model.BlobProperty()
      k = model.KeyProperty()
    BVE = datastore_errors.BadValueError
    a = All()

    a.s = None
    a.s = 'abc'
    a.s = u'def'
    a.s = '\xff'  # Not UTF-8.
    self.assertRaises(BVE, setattr, a, 's', 0)

    a.i = None
    a.i = 42
    a.i = 123L
    self.assertRaises(BVE, setattr, a, 'i', '')

    a.f = None
    a.f = 42
    a.f = 3.14
    self.assertRaises(BVE, setattr, a, 'f', '')

    a.t = None
    a.t = 'abc'
    a.t = u'def'
    a.t = '\xff'  # Not UTF-8.
    self.assertRaises(BVE, setattr, a, 't', 0)

    a.b = None
    a.b = 'abc'
    a.b = '\xff'
    self.assertRaises(BVE, setattr, a, 'b', u'')
    self.assertRaises(BVE, setattr, a, 'b', u'')

    a.k = None
    a.k = model.Key('Foo', 42)
    self.assertRaises(BVE, setattr, a, 'k', '')

  def testLocalStructuredProperty(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()
    class Person(model.Model):
      name = model.StringProperty()
      address = model.LocalStructuredProperty(Address)

    p = Person()
    p.name = 'Google'
    a = Address(street='1600 Amphitheatre')
    p.address = a
    p.address.city = 'Mountain View'
    self.assertEqual(Person.name._get_value(p), 'Google')
    self.assertEqual(p.name, 'Google')
    self.assertEqual(Person.address._get_value(p), a)
    self.assertEqual(Address.street._get_value(a), '1600 Amphitheatre')
    self.assertEqual(Address.city._get_value(a), 'Mountain View')

    pb = p._to_pb()
    # TODO: Validate pb

    # Check we can enable and disable compression and have old data still
    # be understood.
    Person.address._compressed = True
    p = Person._from_pb(pb)
    self.assertEqual(p.name, 'Google')
    self.assertEqual(p.address.street, '1600 Amphitheatre')
    self.assertEqual(p.address.city, 'Mountain View')
    self.assertEqual(p.address, a)
    self.assertEqual(repr(Person.address),
                     "LocalStructuredProperty(Address, 'address', "
                     "compressed=True)")
    pb = p._to_pb()

    Person.address._compressed = False
    p = Person._from_pb(pb)

    # Now try with an empty address
    p = Person()
    p.name = 'Google'
    self.assertTrue(p.address is None)
    pb = p._to_pb()
    p = Person._from_pb(pb)
    self.assertTrue(p.address is None)
    self.assertEqual(p.name, 'Google')

  def testLocalStructuredPropertyCompressed(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()
    class Person(model.Model):
      name = model.StringProperty()
      address = model.LocalStructuredProperty(Address, compressed=True)

    k = model.Key('Person', 'google')
    p = Person(key=k)
    p.name = 'Google'
    p.address = Address(street='1600 Amphitheatre', city='Mountain View')
    p.put()

    # Putting and getting to test compression and deserialization.
    p = k.get()
    p.put()

    p = k.get()
    self.assertEqual(p.name, 'Google')
    self.assertEqual(p.address.street, '1600 Amphitheatre')
    self.assertEqual(p.address.city, 'Mountain View')

  def testLocalStructuredPropertyRepeated(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()
    class Person(model.Model):
      name = model.StringProperty()
      address = model.LocalStructuredProperty(Address, repeated=True)

    k = model.Key('Person', 'google')
    p = Person(key=k)
    p.name = 'Google'
    p.address.append(Address(street='1600 Amphitheatre', city='Mountain View'))
    p.address.append(Address(street='Webb crater', city='Moon'))
    p.put()

    # Putting and getting to test compression and deserialization.
    p = k.get()
    p.put()

    p = k.get()
    self.assertEqual(p.name, 'Google')
    self.assertEqual(p.address[0].street, '1600 Amphitheatre')
    self.assertEqual(p.address[0].city, 'Mountain View')
    self.assertEqual(p.address[1].street, 'Webb crater')
    self.assertEqual(p.address[1].city, 'Moon')

  def testLocalStructuredPropertyRepeatedCompressed(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()
    class Person(model.Model):
      name = model.StringProperty()
      address = model.LocalStructuredProperty(Address, repeated=True,
                                              compressed=True)

    k = model.Key('Person', 'google')
    p = Person(key=k)
    p.name = 'Google'
    p.address.append(Address(street='1600 Amphitheatre', city='Mountain View'))
    p.address.append(Address(street='Webb crater', city='Moon'))
    p.put()

    # Putting and getting to test compression and deserialization.
    p = k.get()
    p.put()

    p = k.get()
    self.assertEqual(p.name, 'Google')
    self.assertEqual(p.address[0].street, '1600 Amphitheatre')
    self.assertEqual(p.address[0].city, 'Mountain View')
    self.assertEqual(p.address[1].street, 'Webb crater')
    self.assertEqual(p.address[1].city, 'Moon')

  def testEmptyList(self):
    class Person(model.Model):
      name = model.StringProperty(repeated=True)
    p = Person()
    self.assertEqual(p.name, [])
    pb = p._to_pb()
    q = Person._from_pb(pb)
    self.assertEqual(q.name, [], str(pb))

  def testEmptyListSerialized(self):
    class Person(model.Model):
      name = model.StringProperty(repeated=True)
    p = Person()
    pb = p._to_pb()
    q = Person._from_pb(pb)
    self.assertEqual(q.name, [], str(pb))

  def testDatetimeSerializing(self):
    class Person(model.Model):
      t = model.GenericProperty()
    p = Person(t=datetime.datetime.utcnow())
    pb = p._to_pb()
    q = Person._from_pb(pb)
    self.assertEqual(p.t, q.t)

  def testExpandoKey(self):
    class Ex(model.Expando):
      pass
    e = Ex()
    self.assertEqual(e.key, None)
    k = model.Key('Ex', 'abc')
    e.key = k
    self.assertEqual(e.key, k)
    k2 = model.Key('Ex', 'def')
    e2 = Ex(key=k2)
    self.assertEqual(e2.key, k2)
    e2.key = k
    self.assertEqual(e2.key, k)
    self.assertEqual(e, e2)
    del e.key
    self.assertEqual(e.key, None)

  def testExpandoRead(self):
    class Person(model.Model):
      name = model.StringProperty()
      city = model.StringProperty()
    p = Person(name='Guido', city='SF')
    pb = p._to_pb()
    q = model.Expando._from_pb(pb)
    self.assertEqual(q.name, 'Guido')
    self.assertEqual(q.city, 'SF')

  def testExpandoWrite(self):
    k = model.Key(flat=['Model', 42])
    p = model.Expando(key=k)
    p.k = k
    p.p = 42
    p.q = 'hello'
    p.u = TESTUSER
    p.d = 2.5
    p.b = True
    p.xy = AMSTERDAM
    pb = p._to_pb()
    self.assertEqual(str(pb), GOLDEN_PB)

  def testExpandoDelAttr(self):
    class Ex(model.Expando):
      static = model.StringProperty()

    e = Ex()
    self.assertEqual(e.static, None)
    self.assertRaises(AttributeError, getattr, e, 'dynamic')
    self.assertRaises(AttributeError, getattr, e, '_absent')

    e.static = 'a'
    e.dynamic = 'b'
    self.assertEqual(e.static, 'a')
    self.assertEqual(e.dynamic, 'b')

    e = Ex(static='a', dynamic='b')
    self.assertEqual(e.static, 'a')
    self.assertEqual(e.dynamic, 'b')

    del e.static
    del e.dynamic
    self.assertEqual(e.static, None)
    self.assertRaises(AttributeError, getattr, e, 'dynamic')

  def testExpandoRepr(self):
    class Person(model.Expando):
      name = model.StringProperty('Name')
      city = model.StringProperty('City')
    p = Person(name='Guido', zip='00000')
    p.city= 'SF'
    self.assertEqual(repr(p),
                     "Person(city='SF', name='Guido', zip='00000')")
    # White box confirmation.
    self.assertEqual(p._values,
                     {'City': 'SF', 'Name': 'Guido', 'zip': '00000'})

  def testExpandoNested(self):
    p = model.Expando()
    nest = model.Expando()
    nest.foo = 42
    nest.bar = 'hello'
    p.nest = nest
    self.assertEqual(p.nest.foo, 42)
    self.assertEqual(p.nest.bar, 'hello')
    pb = p._to_pb()
    q = model.Expando._from_pb(pb)
    self.assertEqual(q.nest.foo, 42)
    self.assertEqual(q.nest.bar, 'hello')

  def testExpandoSubclass(self):
    class Person(model.Expando):
      name = model.StringProperty()
    p = Person()
    p.name = 'Joe'
    p.age = 7
    self.assertEqual(p.name, 'Joe')
    self.assertEqual(p.age, 7)

  def testExpandoConstructor(self):
    p = model.Expando(foo=42, bar='hello')
    self.assertEqual(p.foo, 42)
    self.assertEqual(p.bar, 'hello')
    pb = p._to_pb()
    q = model.Expando._from_pb(pb)
    self.assertEqual(q.foo, 42)
    self.assertEqual(q.bar, 'hello')

  def testExpandoNestedConstructor(self):
    p = model.Expando(foo=42, bar=model.Expando(hello='hello'))
    self.assertEqual(p.foo, 42)
    self.assertEqual(p.bar.hello, 'hello')
    pb = p._to_pb()
    q = model.Expando._from_pb(pb)
    self.assertEqual(q.foo, 42)
    self.assertEqual(q.bar.hello, 'hello')

  def testComputedProperty(self):
    class ComputedTest(model.Model):
      name = model.StringProperty()
      name_lower = model.ComputedProperty(lambda self: self.name.lower())

      @model.ComputedProperty
      def length(self):
        return len(self.name)

      def _compute_hash(self):
        return hash(self.name)
      hash = model.ComputedProperty(_compute_hash, name='hashcode')

    m = ComputedTest(name='Foobar')
    pb = m._to_pb()

    for p in pb.property_list():
      if p.name() == 'name_lower':
        self.assertEqual(p.value().stringvalue(), 'foobar')
        break
    else:
      self.assert_(False, "name_lower not found in PB")

    m = ComputedTest._from_pb(pb)
    self.assertEqual(m.name, 'Foobar')
    self.assertEqual(m.name_lower, 'foobar')
    self.assertEqual(m.length, 6)
    self.assertEqual(m.hash, hash('Foobar'))

  def testLargeValues(self):
    class Demo(model.Model):
      bytes = model.BlobProperty()
      text = model.TextProperty()
    x = Demo(bytes='x'*1000, text=u'a'*1000)
    key = x.put()
    y = key.get()
    self.assertEqual(x, y)
    self.assertTrue(isinstance(y.bytes, str))
    self.assertTrue(isinstance(y.text, unicode))

  def testMultipleStructuredProperty(self):
    class Address(model.Model):
      label = model.StringProperty()
      text = model.StringProperty()
    class Person(model.Model):
      name = model.StringProperty()
      address = model.StructuredProperty(Address, repeated=True)

    m = Person(name='Google',
               address=[Address(label='work', text='San Francisco'),
                        Address(label='home', text='Mountain View')])
    m.key = model.Key(flat=['Person', None])
    self.assertEqual(m.address[0].label, 'work')
    self.assertEqual(m.address[0].text, 'San Francisco')
    self.assertEqual(m.address[1].label, 'home')
    self.assertEqual(m.address[1].text, 'Mountain View')
    [k] = self.conn.put([m])
    m.key = k  # Connection.put() doesn't do this!
    [m2] = self.conn.get([k])
    self.assertEqual(m2, m)

  def testIdAndParentPut(self):
    # id
    m = model.Model(id='bar')
    self.assertEqual(m.put(), model.Key('Model', 'bar'))

    # id + parent
    p = model.Key('ParentModel', 'foo')
    m = model.Model(id='bar', parent=p)
    self.assertEqual(m.put(), model.Key('ParentModel', 'foo', 'Model', 'bar'))

    # parent without id
    p = model.Key('ParentModel', 'foo')
    m = model.Model(parent=p)
    m.put()
    self.assertTrue(m.key.id())

  def testAllocateIds(self):
    class MyModel(model.Model):
      pass

    res = MyModel.allocate_ids(size=100)
    self.assertEqual(res, (1, 100))

    # with parent
    key = model.Key(flat=(MyModel._get_kind(), 1))
    res = MyModel.allocate_ids(size=200, parent=key)
    self.assertEqual(res, (101, 300))

  def testGetOrInsert(self):
    class MyModel(model.Model):
      text = model.StringProperty()

    key = model.Key(flat=(MyModel._get_kind(), 'baz'))
    self.assertEqual(key.get(), None)

    MyModel.get_or_insert('baz', text='baz')
    self.assertNotEqual(key.get(), None)
    self.assertEqual(key.get().text, 'baz')

  def testGetById(self):
    class MyModel(model.Model):
      pass

    kind = MyModel._get_kind()

    # key id
    ent1 = MyModel(key=model.Key(pairs=[(kind, 1)]))
    key = ent1.put()
    res = MyModel.get_by_id(1)
    self.assertEqual(res, ent1)

    # key name
    ent2 = MyModel(key=model.Key(pairs=[(kind, 'foo')]))
    key = ent2.put()
    res = MyModel.get_by_id('foo')
    self.assertEqual(res, ent2)

    # key id + parent
    ent3 = MyModel(key=model.Key(pairs=[(kind, 1), (kind, 2)]))
    key = ent3.put()
    res = MyModel.get_by_id(2, parent=model.Key(pairs=[(kind, 1)]))
    self.assertEqual(res, ent3)

    # key name + parent
    ent4 = MyModel(key=model.Key(pairs=[(kind, 1), (kind, 'bar')]))
    key = ent4.put()
    res = MyModel.get_by_id('bar', parent=ent1.key)
    self.assertEqual(res, ent4)

    # None
    res = MyModel.get_by_id('idontexist')
    self.assertEqual(res, None)

    # Invalid parent
    self.assertRaises(datastore_errors.BadValueError, MyModel.get_by_id,
                      'bar', parent=1)

  def testDelete(self):
    class MyModel(model.Model):
      pass

    ent1 = MyModel()
    key1 = ent1.put()
    ent2 = key1.get()
    self.assertEqual(ent1, ent2)
    key1.delete()
    ent3 = key1.get()
    self.assertEqual(ent3, None)

  def testPopulate(self):
    class MyModel(model.Model):
      name = model.StringProperty()
    m = MyModel()
    m.populate(name='abc')
    self.assertEqual(m.name, 'abc')
    m.populate(name='def')
    self.assertEqual(m.name, 'def')
    self.assertRaises(AttributeError, m.populate, foo=42)

  def testPopulate_Expando(self):
    class Ex(model.Expando):
      name = model.StringProperty()
    m = Ex()
    m.populate(name='abc')
    self.assertEqual(m.name, 'abc')
    m.populate(foo=42)
    self.assertEqual(m.foo, 42)

  def testTransaction(self):
    class MyModel(model.Model):
      text = model.StringProperty()

    key = model.Key(MyModel, 'babaz')
    self.assertEqual(key.get(), None)

    def callback():
      # Emulate get_or_insert()
      a = key.get()
      if a is None:
        a = MyModel(text='baz', key=key)
        a.put()
      return a

    b = model.transaction(callback)
    self.assertNotEqual(b, None)
    self.assertEqual(b.text, 'baz')
    self.assertEqual(key.get(), b)

    key = model.Key(MyModel, 'bababaz')
    self.assertEqual(key.get(), None)
    c = model.transaction(callback, retry=0, entity_group=key)
    self.assertNotEqual(c, None)
    self.assertEqual(c.text, 'baz')
    self.assertEqual(key.get(), c)

  def testGetMultiAsync(self):
    model.Model._kind_map['Model'] = model.Model
    ent1 = model.Model(key=model.Key('Model', 1))
    ent2 = model.Model(key=model.Key('Model', 2))
    ent3 = model.Model(key=model.Key('Model', 3))
    key1 = ent1.put()
    key2 = ent2.put()
    key3 = ent3.put()

    @tasklets.tasklet
    def foo():
        ents = yield model.get_multi_async([key1, key2, key3])
        raise tasklets.Return(ents)

    res = foo().get_result()
    self.assertEqual(res, [ent1, ent2, ent3])

  def testGetMulti(self):
    model.Model._kind_map['Model'] = model.Model
    ent1 = model.Model(key=model.Key('Model', 1))
    ent2 = model.Model(key=model.Key('Model', 2))
    ent3 = model.Model(key=model.Key('Model', 3))
    key1 = ent1.put()
    key2 = ent2.put()
    key3 = ent3.put()

    res = model.get_multi((key1, key2, key3))
    self.assertEqual(res, [ent1, ent2, ent3])

  def testPutMultiAsync(self):
    ent1 = model.Model(key=model.Key('Model', 1))
    ent2 = model.Model(key=model.Key('Model', 2))
    ent3 = model.Model(key=model.Key('Model', 3))

    @tasklets.tasklet
    def foo():
        ents = yield model.put_multi_async([ent1, ent2, ent3])
        raise tasklets.Return(ents)

    res = foo().get_result()
    self.assertEqual(res, [ent1.key, ent2.key, ent3.key])

  def testPutMulti(self):
    ent1 = model.Model(key=model.Key('Model', 1))
    ent2 = model.Model(key=model.Key('Model', 2))
    ent3 = model.Model(key=model.Key('Model', 3))

    res = model.put_multi((ent1, ent2, ent3))
    self.assertEqual(res, [ent1.key, ent2.key, ent3.key])

  def testDeleteMultiAsync(self):
    model.Model._kind_map['Model'] = model.Model
    ent1 = model.Model(key=model.Key('Model', 1))
    ent2 = model.Model(key=model.Key('Model', 2))
    ent3 = model.Model(key=model.Key('Model', 3))
    key1 = ent1.put()
    key2 = ent2.put()
    key3 = ent3.put()

    self.assertEqual(key1.get(), ent1)
    self.assertEqual(key2.get(), ent2)
    self.assertEqual(key3.get(), ent3)

    @tasklets.tasklet
    def foo():
        ents = yield model.delete_multi_async([key1, key2, key3])
        raise tasklets.Return(ents)

    res = foo().get_result()
    self.assertEqual(key1.get(), None)
    self.assertEqual(key2.get(), None)
    self.assertEqual(key3.get(), None)

  def testDeleteMulti(self):
    model.Model._kind_map['Model'] = model.Model
    ent1 = model.Model(key=model.Key('Model', 1))
    ent2 = model.Model(key=model.Key('Model', 2))
    ent3 = model.Model(key=model.Key('Model', 3))
    key1 = ent1.put()
    key2 = ent2.put()
    key3 = ent3.put()

    self.assertEqual(key1.get(), ent1)
    self.assertEqual(key2.get(), ent2)
    self.assertEqual(key3.get(), ent3)

    res = model.delete_multi((key1, key2, key3))

    self.assertEqual(key1.get(), None)
    self.assertEqual(key2.get(), None)
    self.assertEqual(key3.get(), None)

  def testNamespaces(self):
    save_namespace = namespace_manager.get_namespace()
    try:
      namespace_manager.set_namespace('ns1')
      k1 = model.Key('A', 1)
      self.assertEqual(k1.namespace(), 'ns1')
      k2 = model.Key('B', 2, namespace='ns2')
      self.assertEqual(k2.namespace(), 'ns2')
      namespace_manager.set_namespace('ns3')
      self.assertEqual(k1.namespace(), 'ns1')
      k3 = model.Key('C', 3, parent=k1)
      self.assertEqual(k3.namespace(), 'ns1')

      # Test that namespaces survive serialization
      namespace_manager.set_namespace('ns2')
      km = model.Key('M', 1, namespace='ns4')
      class M(model.Model):
        keys = model.KeyProperty(repeated=True)
      m1 = M(keys=[k1, k2, k3], key=km)
      pb = m1._to_pb()
      namespace_manager.set_namespace('ns3')
      m2 = M._from_pb(pb)
      self.assertEqual(m1, m2)
      self.assertEqual(m2.keys[0].namespace(), 'ns1')
      self.assertEqual(m2.keys[1].namespace(), 'ns2')
      self.assertEqual(m2.keys[2].namespace(), 'ns1')

      # Now test the same thing for Expando
      namespace_manager.set_namespace('ns2')
      ke = model.Key('E', 1)
      class E(model.Expando):
        pass
      e1 = E(keys=[k1, k2, k3], key=ke)
      pb = e1._to_pb()
      namespace_manager.set_namespace('ns3')
      e2 = E._from_pb(pb)
      self.assertEqual(e1, e2)

      # Test that an absent namespace always means the empty namespace
      namespace_manager.set_namespace('')
      k3 = model.Key('E', 2)
      e3 = E(key=k3, k=k3)
      pb = e3._to_pb()
      namespace_manager.set_namespace('ns4')
      e4 = E._from_pb(pb)
      self.assertEqual(e4.key.namespace(), '')
      self.assertEqual(e4.k.namespace(), '')

    finally:
      namespace_manager.set_namespace(save_namespace)

  def testOverrideModelKey(self):
    class MyModel(model.Model):
      # key, overriden
      key = model.StringProperty()
      # aha, here it is!
      real_key = model.ModelKey()

    class MyExpando(model.Expando):
      # key, overriden
      key = model.StringProperty()
      # aha, here it is!
      real_key = model.ModelKey()

    m = MyModel()
    k = model.Key('MyModel', 'foo')
    m.key = 'bar'
    m.real_key = k
    m.put()

    res = k.get()
    self.assertEqual(res, m)
    self.assertEqual(res.key, 'bar')
    self.assertEqual(res.real_key, k)

    q = MyModel.query(MyModel.real_key == k)
    res = q.get()
    self.assertEqual(res, m)
    self.assertEqual(res.key, 'bar')
    self.assertEqual(res.real_key, k)

    m = MyExpando()
    k = model.Key('MyExpando', 'foo')
    m.key = 'bar'
    m.real_key = k
    m.put()

    res = k.get()
    self.assertEqual(res, m)
    self.assertEqual(res.key, 'bar')
    self.assertEqual(res.real_key, k)

    q = MyExpando.query(MyModel.real_key == k)
    res = q.get()
    self.assertEqual(res, m)
    self.assertEqual(res.key, 'bar')
    self.assertEqual(res.real_key, k)

  def testTransactionalDecorator(self):
    # This tests @model.transactional and model.in_transaction(), and
    # indirectly context.Context.in_transaction().
    logs = []
    @model.transactional
    def foo(a, b):
      self.assertTrue(model.in_transaction())
      logs.append(tasklets.get_context()._conn)  # White box
      return a + b
    @model.transactional
    def bar(a):
      self.assertTrue(model.in_transaction())
      logs.append(tasklets.get_context()._conn)  # White box
      return foo(a, 42)
    before = tasklets.get_context()._conn
    self.assertFalse(model.in_transaction())
    x = bar(100)
    self.assertFalse(model.in_transaction())
    after = tasklets.get_context()._conn
    self.assertEqual(before, after)
    self.assertEqual(x, 142)
    self.assertEqual(len(logs), 2)
    self.assertEqual(logs[0], logs[1])
    self.assertNotEqual(before, logs[0])

  def testPropertyFilters(self):
    class M(model.Model):
      dt = model.DateTimeProperty()
      d = model.DateProperty()
      t = model.TimeProperty()
      f = model.FloatProperty()
      s = model.StringProperty()
      k = model.KeyProperty()
      b = model.BooleanProperty()
      i = model.IntegerProperty()
      g = model.GeoPtProperty()
      @model.ComputedProperty
      def c(self):
        return self.i + 1
      u = model.UserProperty()

    values = {
      'dt': datetime.datetime.now(),
      'd': datetime.date.today(),
      't': datetime.datetime.now().time(),
      'f': 4.2,
      's': 'foo',
      'k': model.Key('Foo', 'bar'),
      'b': False,
      'i': 42,
      'g': AMSTERDAM,
      'u': TESTUSER,
    }

    m = M(**values)
    m.put()

    q = M.query(M.dt == values['dt'])
    self.assertEqual(q.get(), m)

    q = M.query(M.d == values['d'])
    self.assertEqual(q.get(), m)

    q = M.query(M.t == values['t'])
    self.assertEqual(q.get(), m)

    q = M.query(M.f == values['f'])
    self.assertEqual(q.get(), m)

    q = M.query(M.s == values['s'])
    self.assertEqual(q.get(), m)

    q = M.query(M.k == values['k'])
    self.assertEqual(q.get(), m)

    q = M.query(M.b == values['b'])
    self.assertEqual(q.get(), m)

    q = M.query(M.i == values['i'])
    self.assertEqual(q.get(), m)

    q = M.query(M.g == values['g'])
    self.assertEqual(q.get(), m)

    q = M.query(M.c == values['i'] + 1)
    self.assertEqual(q.get(), m)

    q = M.query(M.u == values['u'])
    self.assertEqual(q.get(), m)


def main():
  unittest.main()

if __name__ == '__main__':
  main()
