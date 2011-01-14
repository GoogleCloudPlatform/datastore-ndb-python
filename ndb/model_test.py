"""Tests for model.py."""

import base64
import datetime
import difflib
import pickle
import re
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_errors
from google.appengine.api import datastore_file_stub
from google.appengine.datastore import entity_pb

from ndb import model, query

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
  name: "b"
  value <
    stringValue: "\\000\\377"
  >
  multiple: false
>
raw_property <
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
  name: "root.left.left.name"
  value <
    stringValue: "a1a"
  >
  multiple: false
>
raw_property <
  name: "root.left.name"
  value <
    stringValue: "a1"
  >
  multiple: false
>
raw_property <
  name: "root.left.rite.name"
  value <
    stringValue: "a1b"
  >
  multiple: false
>
raw_property <
  name: "root.name"
  value <
    stringValue: "a"
  >
  multiple: false
>
raw_property <
  name: "root.rite.name"
  value <
    stringValue: "a2"
  >
  multiple: false
>
raw_property <
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

class ModelTests(unittest.TestCase):

  def setUp(self):
    model.Model.ResetKindMap()
    self.conn = model.make_connection()

  def tearDown(self):
    self.assertTrue(model.Model._properties == {})
    self.assertTrue(model.Expando._properties == {})
    model.Model.ResetKindMap()

  def testKey(self):
    m = model.Model()
    self.assertEqual(m.key, None)
    k = model.Key(flat=['ParentModel', 42, 'Model', 'foobar'])
    m.key = k
    self.assertEqual(m.key, k)
    del m.key
    self.assertEqual(m.key, None)

  def testIncompleteKey(self):
    m = model.Model()
    k = model.Key(flat=['Model', None])
    m.key = k
    pb = m.ToPb()
    m2 = model.Model()
    m2.FromPb(pb)
    self.assertEqual(m2, m)

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
      p = model.IntegerProperty()
      q = model.StringProperty()
      d = model.FloatProperty()
      k = model.KeyProperty()

    ent = MyModel()
    k = model.Key(flat=['MyModel', 42])
    ent.key = k
    MyModel.p.SetValue(ent, 42)
    MyModel.q.SetValue(ent, 'hello')
    MyModel.d.SetValue(ent, 2.5)
    MyModel.k.SetValue(ent, k)
    self.assertEqual(MyModel.p.GetValue(ent), 42)
    self.assertEqual(MyModel.q.GetValue(ent), 'hello')
    self.assertEqual(MyModel.d.GetValue(ent), 2.5)
    self.assertEqual(MyModel.k.GetValue(ent), k)
    pb = self.conn.adapter.entity_to_pb(ent)
    self.assertEqual(str(pb), INDEXED_PB)

    ent = MyModel()
    ent.FromPb(pb)
    self.assertEqual(ent.GetKind(), 'MyModel')
    k = model.Key(flat=['MyModel', 42])
    self.assertEqual(ent.key, k)
    self.assertEqual(MyModel.p.GetValue(ent), 42)
    self.assertEqual(MyModel.q.GetValue(ent), 'hello')
    self.assertEqual(MyModel.d.GetValue(ent), 2.5)
    self.assertEqual(MyModel.k.GetValue(ent), k)

  def testUnindexedProperty(self):
    class MyModel(model.Model):
      t = model.TextProperty()
      b = model.BlobProperty()

    ent = MyModel()
    MyModel.t.SetValue(ent, u'Hello world\u1234')
    MyModel.b.SetValue(ent, '\x00\xff')
    self.assertEqual(MyModel.t.GetValue(ent), u'Hello world\u1234')
    self.assertEqual(MyModel.b.GetValue(ent), '\x00\xff')
    pb = ent.ToPb()
    self.assertEqual(str(pb), UNINDEXED_PB)

    ent = MyModel()
    ent.FromPb(pb)
    self.assertEqual(ent.GetKind(), 'MyModel')
    k = model.Key(flat=['MyModel', None])
    self.assertEqual(ent.key, k)
    self.assertEqual(MyModel.t.GetValue(ent), u'Hello world\u1234')
    self.assertEqual(MyModel.b.GetValue(ent), '\x00\xff')

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
    self.assertEqual(Person.name.GetValue(p), 'Google')
    self.assertEqual(p.name, 'Google')
    self.assertEqual(Person.address.GetValue(p), a)
    self.assertEqual(Address.street.GetValue(a), '1600 Amphitheatre')
    self.assertEqual(Address.city.GetValue(a), 'Mountain View')

    pb = p.ToPb()
    self.assertEqual(str(pb), PERSON_PB)

    p = Person()
    p.FromPb(pb)
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
    pb = p.ToPb()
    self.assertEqual(str(pb), NESTED_PB)

    p = Person()
    p.FromPb(pb)
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
    Node.FixUpProperties()
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
    pb = tree.ToPb()
    self.assertEqual(str(pb), RECURSIVE_PB)

    tree2 = Tree()
    tree2.FromPb(pb)
    self.assertEqual(tree2, tree)

  def testRenamedProperty(self):
    class MyModel(model.Model):
      pp = model.IntegerProperty('p')
      qq = model.StringProperty('q')
      dd = model.FloatProperty('d')
      kk = model.KeyProperty('k')

    ent = MyModel()
    k = model.Key(flat=['MyModel', 42])
    ent.key = k
    MyModel.pp.SetValue(ent, 42)
    MyModel.qq.SetValue(ent, 'hello')
    MyModel.dd.SetValue(ent, 2.5)
    MyModel.kk.SetValue(ent, k)
    self.assertEqual(MyModel.pp.GetValue(ent), 42)
    self.assertEqual(MyModel.qq.GetValue(ent), 'hello')
    self.assertEqual(MyModel.dd.GetValue(ent), 2.5)
    self.assertEqual(MyModel.kk.GetValue(ent), k)
    pb = self.conn.adapter.entity_to_pb(ent)
    self.assertEqual(str(pb), INDEXED_PB)

    ent = MyModel()
    ent.FromPb(pb)
    self.assertEqual(ent.GetKind(), 'MyModel')
    k = model.Key(flat=['MyModel', 42])
    self.assertEqual(ent.key, k)
    self.assertEqual(MyModel.pp.GetValue(ent), 42)
    self.assertEqual(MyModel.qq.GetValue(ent), 'hello')
    self.assertEqual(MyModel.dd.GetValue(ent), 2.5)
    self.assertEqual(MyModel.kk.GetValue(ent), k)

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
    pb = p.ToPb()
    self.assertEqual(str(pb), NESTED_PB)

    p = Person()
    p.FromPb(pb)
    self.assertEqual(p.na, 'Google')
    self.assertEqual(p.ad.ho.st, '1600 Amphitheatre')
    self.assertEqual(p.ad.ho.ci, 'Mountain View')
    self.assertEqual(p.ad.wo.st, '345 Spear')
    self.assertEqual(p.ad.wo.ci, 'San Francisco')

  def testKindMap(self):
    model.Model.ResetKindMap()
    class A1(model.Model):
      pass
    self.assertEqual(model.Model.GetKindMap(), {'A1': A1})
    class A2(model.Model):
      pass
    self.assertEqual(model.Model.GetKindMap(), {'A1': A1, 'A2': A2})

  def testMultipleProperty(self):
    class Person(model.Model):
      name = model.StringProperty()
      address = model.StringProperty(repeated=True)

    m = Person(name='Google', address=['345 Spear', 'San Francisco'])
    m.key = model.Key(flat=['Person', None])
    self.assertEqual(m.address, ['345 Spear', 'San Francisco'])
    pb = m.ToPb()
    self.assertEqual(str(pb), MULTI_PB)

    m2 = Person()
    m2.FromPb(pb)
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
    pb = m.ToPb()
    self.assertEqual(str(pb), MULTIINSTRUCT_PB)

    m2 = Person()
    m2.FromPb(pb)
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
    pb = m.ToPb()
    self.assertEqual(str(pb), MULTISTRUCT_PB)

    m2 = Person()
    m2.FromPb(pb)
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
    pb = p.ToPb()
    q = Person()
    q.FromPb(pb)
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
    pb = p.ToPb()
    q = model.Model()
    q.FromPb(pb)
    qb = q.ToPb()
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
    self.assertEqual(Person.name.GetValue(p), 'Google')
    self.assertEqual(p.name, 'Google')
    self.assertEqual(Person.address.GetValue(p), a)
    self.assertEqual(Address.street.GetValue(a), '1600 Amphitheatre')
    self.assertEqual(Address.city.GetValue(a), 'Mountain View')

    pb = p.ToPb()
    # TODO: Validate pb

    p = Person()
    p.FromPb(pb)
    self.assertEqual(p.name, 'Google')
    self.assertEqual(p.address.street, '1600 Amphitheatre')
    self.assertEqual(p.address.city, 'Mountain View')
    self.assertEqual(p.address, a)

    # Now try with an empty address
    p = Person()
    p.name = 'Google'
    self.assertTrue(p.address is None)
    pb = p.ToPb()
    p = Person()
    p.FromPb(pb)
    self.assertTrue(p.address is None)
    self.assertEqual(p.name, 'Google')

  def testEmptyList(self):
    class Person(model.Model):
      name = model.StringProperty(repeated=True)
    p = Person()
    self.assertEqual(p.name, [])
    pb = p.ToPb()
    q = Person()
    q.FromPb(pb)
    self.assertEqual(q.name, [], str(pb))

  def testEmptyListSerialized(self):
    class Person(model.Model):
      name = model.StringProperty(repeated=True)
    p = Person()
    pb = p.ToPb()
    q = Person()
    q.FromPb(pb)
    self.assertEqual(q.name, [], str(pb))

  def testDatetimeSerializing(self):
    class Person(model.Model):
      t = model.GenericProperty()
    p = Person(t=datetime.datetime.utcnow())
    pb = p.ToPb()
    q = Person()
    q.FromPb(pb)
    self.assertEqual(p.t, q.t)

  def testExpandoRead(self):
    class Person(model.Model):
      name = model.StringProperty()
      city = model.StringProperty()
    p = Person(name='Guido', city='SF')
    pb = p.ToPb()
    q = model.Expando()
    q.FromPb(pb)
    self.assertEqual(q.name, 'Guido')
    self.assertEqual(q.city, 'SF')

  def testExpandoWrite(self):
    k = model.Key(flat=['Model', 42])
    p = model.Expando(key=k)
    p.k = k
    p.p = 42
    p.q = 'hello'
    p.d = 2.5
    pb = p.ToPb()
    self.assertEqual(str(pb), GOLDEN_PB)

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
    pb = p.ToPb()
    q = model.Expando()
    q.FromPb(pb)
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
    pb = p.ToPb()
    q = model.Expando()
    q.FromPb(pb)
    self.assertEqual(q.foo, 42)
    self.assertEqual(q.bar, 'hello')

  def testExpandoNestedConstructor(self):
    p = model.Expando(foo=42, bar=model.Expando(hello='hello'))
    self.assertEqual(p.foo, 42)
    self.assertEqual(p.bar.hello, 'hello')
    pb = p.ToPb()
    q = model.Expando()
    q.FromPb(pb)
    self.assertEqual(q.foo, 42)
    self.assertEqual(q.bar.hello, 'hello')

class DatastoreTests(unittest.TestCase):
  """Tests that actually interact with the (stub) Datastore."""
  # TODO: Merge this into ModelTests so there's less code duplication.

  def setUp(self):
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    stub = datastore_file_stub.DatastoreFileStub('_', None)
    apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', stub)
    self.conn = model.make_connection()

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

def main():
  unittest.main()

if __name__ == '__main__':
  main()
