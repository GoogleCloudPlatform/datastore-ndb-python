"""Tests for model.py."""

import base64
import pickle
import re
import unittest

from google.appengine.datastore import entity_pb

from ndb import key, model

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

class ModelTests(unittest.TestCase):

  def testProperties(self):
    m = model.Model()
    self.assertEqual(m.propnames(), [])
    self.assertEqual(m.getvalue('p'), None)
    m.setvalue('p', 42)
    self.assertEqual(m.getvalue('p'), 42)
    self.assertEqual(m.propnames(), ['p'])
    m.delvalue('p')
    self.assertEqual(m.propnames(), [])
    self.assertEqual(m.getvalue('p'), None)

  def testKey(self):
    m = model.Model()
    self.assertEqual(m.key, None)
    k = key.Key(flat=['ParentModel', 42, 'Model', 'foobar'])
    m.key = k
    self.assertEqual(m.key, k)
    del m.key
    self.assertEqual(m.key, None)

  def testSerialize(self):
    m = model.Model()
    k = key.Key(flat=['Model', 42])
    m.key = k
    m.setvalue('p', 42)
    m.setvalue('q', 'hello')
    m.setvalue('k', key.Key(flat=['Model', 42]))
    pb = m.ToPb()
    self.assertEqual(str(pb), GOLDEN_PB)
    m2 = model.Model()
    m2.FromPb(pb)
    self.assertEqual(str(m2.ToPb()), GOLDEN_PB)

  def testIncompleteKey(self):
    m = model.Model()
    k = key.Key(flat=['Model', None])
    m.key = k
    m.setvalue('p', 42)
    pb = m.ToPb()
    m2 = model.Model()
    m2.FromPb(pb)
    self.assertEqual(m2, m)

  def testPropertySerializeDeserialize(self):
    class MyModel(model.Model):
      p = model.IntegerProperty()
      q = model.StringProperty()
      k = model.KeyProperty()
    model.FixUpProperties(MyModel)

    ent = MyModel()
    k = model.Key(flat=['MyModel', 42])
    ent.key = k
    MyModel.p.SetValue(ent, 42)
    MyModel.q.SetValue(ent, 'hello')
    MyModel.k.SetValue(ent, k)
    self.assertEqual(MyModel.p.GetValue(ent), 42)
    self.assertEqual(MyModel.q.GetValue(ent), 'hello')
    self.assertEqual(MyModel.k.GetValue(ent), k)
    pb = model.conn.adapter.entity_to_pb(ent)
    self.assertEqual(str(pb), INDEXED_PB)

    ent = MyModel()
    ent.FromPb(pb)
    self.assertEqual(ent.getkind(), 'MyModel')
    k = model.Key(flat=['MyModel', 42])
    self.assertEqual(ent.key, k)
    self.assertEqual(MyModel.p.GetValue(ent), 42)
    self.assertEqual(MyModel.q.GetValue(ent), 'hello')
    self.assertEqual(MyModel.k.GetValue(ent), k)

  def testUnindexedPropertySerializeDeserialize(self):
    class MyModel(model.Model):
      t = model.TextProperty()
      b = model.BlobProperty()
    model.FixUpProperties(MyModel)

    ent = MyModel()
    MyModel.t.SetValue(ent, u'Hello world\u1234')
    MyModel.b.SetValue(ent, '\x00\xff')
    self.assertEqual(MyModel.t.GetValue(ent), u'Hello world\u1234')
    self.assertEqual(MyModel.b.GetValue(ent), '\x00\xff')
    pb = ent.ToPb()
    self.assertEqual(str(pb), UNINDEXED_PB)

    ent = MyModel()
    ent.FromPb(pb)
    self.assertEqual(ent.getkind(), 'MyModel')
    k = model.Key(flat=['MyModel', None])
    self.assertEqual(ent.key, k)
    self.assertEqual(MyModel.t.GetValue(ent), u'Hello world\u1234')
    self.assertEqual(MyModel.b.GetValue(ent), '\x00\xff')

  def testStructuredProperty(self):
    class Address(model.MiniModel):
      street = model.StringProperty()
      city = model.StringProperty()
    class Person(model.Model):
      name = model.StringProperty()
      address = Address.ToProperty()
    model.FixUpProperties(Person)

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

def main():
  unittest.main()

if __name__ == '__main__':
  main()