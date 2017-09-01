#
# Copyright 2017 The ndb Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from .google_imports import namespace_manager
from .google_imports import memcache

from . import autobatcher
from . import tasklets

class MemcacheClient(object):
  # NOTE: The default memcache prefix is altered if an incompatible change is
  # required. Remember to check release notes when using a custom prefix.
  _memcache_prefix = 'NDB9:'  # TODO: Might make this configurable

  def __init__(self, conn=None, auto_batcher_class=autobatcher.AutoBatcher, max_memcache=None):
    # NOTE: If conn is not None, config is only used to get the
    # auto-batcher limits.
    self._conn = conn
    self._auto_batcher_class = auto_batcher_class

    # Create the memcache auto-batchers.
    self.memcache_get_batcher = auto_batcher_class(self._memcache_get_tasklet, max_memcache)
    self.memcache_set_batcher = auto_batcher_class(self._memcache_set_tasklet, max_memcache)
    self.memcache_del_batcher = auto_batcher_class(self._memcache_del_tasklet, max_memcache)
    self.memcache_off_batcher = auto_batcher_class(self._memcache_off_tasklet, max_memcache)
    self._memcache = memcache.Client()

  @tasklets.tasklet
  def _memcache_get_tasklet(self, todo, options):
    if not todo:
      raise RuntimeError('Nothing to do.')
    for_cas, namespace, deadline = options
    keys = set()
    for unused_fut, key in todo:
      keys.add(key)
    rpc = memcache.create_rpc(deadline=deadline)
    results = yield self._memcache.get_multi_async(keys, for_cas=for_cas,
                                                   namespace=namespace,
                                                   rpc=rpc)
    for fut, key in todo:
      fut.set_result(results.get(key))

  @tasklets.tasklet
  def _memcache_set_tasklet(self, todo, options):
    if not todo:
      raise RuntimeError('Nothing to do.')
    opname, time, namespace, deadline = options
    methodname = opname + '_multi_async'
    method = getattr(self._memcache, methodname)
    mapping = {}
    for unused_fut, (key, value) in todo:
      mapping[key] = value
    rpc = memcache.create_rpc(deadline=deadline)
    results = yield method(mapping, time=time, namespace=namespace, rpc=rpc)
    for fut, (key, unused_value) in todo:
      if results is None:
        status = memcache.MemcacheSetResponse.ERROR
      else:
        status = results.get(key)
      fut.set_result(status == memcache.MemcacheSetResponse.STORED)

  @tasklets.tasklet
  def _memcache_del_tasklet(self, todo, options):
    if not todo:
      raise RuntimeError('Nothing to do.')
    seconds, namespace, deadline = options
    keys = set()
    for unused_fut, key in todo:
      keys.add(key)
    rpc = memcache.create_rpc(deadline=deadline)
    statuses = yield self._memcache.delete_multi_async(keys, seconds=seconds,
                                                       namespace=namespace,
                                                       rpc=rpc)
    status_key_mapping = {}
    if statuses:  # On network error, statuses is None.
      for key, status in zip(keys, statuses):
        status_key_mapping[key] = status
    for fut, key in todo:
      status = status_key_mapping.get(key, memcache.DELETE_NETWORK_FAILURE)
      fut.set_result(status)

  @tasklets.tasklet
  def _memcache_off_tasklet(self, todo, options):
    if not todo:
      raise RuntimeError('Nothing to do.')
    initial_value, namespace, deadline = options
    mapping = {}  # {key: delta}
    for unused_fut, (key, delta) in todo:
      mapping[key] = delta
    rpc = memcache.create_rpc(deadline=deadline)
    results = yield self._memcache.offset_multi_async(
        mapping, initial_value=initial_value, namespace=namespace, rpc=rpc)
    for fut, (key, unused_delta) in todo:
      fut.set_result(results.get(key))

  def memcache_get(self, key, for_cas=False, namespace=None, use_cache=False,
                   deadline=None):
    """An auto-batching wrapper for memcache.get() or .get_multi().

    Args:
      key: Key to set.  This must be a string; no prefix is applied.
      for_cas: If True, request and store CAS ids on the Context.
      namespace: Optional namespace.
      deadline: Optional deadline for the RPC.

    Returns:
      A Future (!) whose return value is the value retrieved from
      memcache, or None.
    """
    if not isinstance(key, basestring):
      raise TypeError('key must be a string; received %r' % key)
    if not isinstance(for_cas, bool):
      raise TypeError('for_cas must be a bool; received %r' % for_cas)
    if namespace is None:
      namespace = namespace_manager.get_namespace()
    options = (for_cas, namespace, deadline)
    batcher = self.memcache_get_batcher
    if use_cache:
      return batcher.add_once(key, options)
    else:
      return batcher.add(key, options)

  # XXX: Docstrings below.

  def memcache_gets(self, key, namespace=None, use_cache=False, deadline=None):
    return self.memcache_get(key, for_cas=True, namespace=namespace,
                             use_cache=use_cache, deadline=deadline)

  def memcache_set(self, key, value, time=0, namespace=None, use_cache=False,
                   deadline=None):
    if not isinstance(key, basestring):
      raise TypeError('key must be a string; received %r' % key)
    if not isinstance(time, (int, long)):
      raise TypeError('time must be a number; received %r' % time)
    if namespace is None:
      namespace = namespace_manager.get_namespace()
    options = ('set', time, namespace, deadline)
    batcher = self.memcache_set_batcher
    if use_cache:
      return batcher.add_once((key, value), options)
    else:
      return batcher.add((key, value), options)

  def memcache_add(self, key, value, time=0, namespace=None, deadline=None):
    if not isinstance(key, basestring):
      raise TypeError('key must be a string; received %r' % key)
    if not isinstance(time, (int, long)):
      raise TypeError('time must be a number; received %r' % time)
    if namespace is None:
      namespace = namespace_manager.get_namespace()
    return self.memcache_set_batcher.add((key, value),
                                          ('add', time, namespace, deadline))

  def memcache_replace(self, key, value, time=0, namespace=None, deadline=None):
    if not isinstance(key, basestring):
      raise TypeError('key must be a string; received %r' % key)
    if not isinstance(time, (int, long)):
      raise TypeError('time must be a number; received %r' % time)
    if namespace is None:
      namespace = namespace_manager.get_namespace()
    options = ('replace', time, namespace, deadline)
    return self.memcache_set_batcher.add((key, value), options)

  def memcache_cas(self, key, value, time=0, namespace=None, deadline=None):
    if not isinstance(key, basestring):
      raise TypeError('key must be a string; received %r' % key)
    if not isinstance(time, (int, long)):
      raise TypeError('time must be a number; received %r' % time)
    if namespace is None:
      namespace = namespace_manager.get_namespace()
    return self.memcache_set_batcher.add((key, value),
                                          ('cas', time, namespace, deadline))

  def memcache_delete(self, key, seconds=0, namespace=None, deadline=None):
    if not isinstance(key, basestring):
      raise TypeError('key must be a string; received %r' % key)
    if not isinstance(seconds, (int, long)):
      raise TypeError('seconds must be a number; received %r' % seconds)
    if namespace is None:
      namespace = namespace_manager.get_namespace()
    return self.memcache_del_batcher.add(key, (seconds, namespace, deadline))

  def memcache_incr(self, key, delta=1, initial_value=None, namespace=None,
                    deadline=None):
    if not isinstance(key, basestring):
      raise TypeError('key must be a string; received %r' % key)
    if not isinstance(delta, (int, long)):
      raise TypeError('delta must be a number; received %r' % delta)
    if initial_value is not None and not isinstance(initial_value, (int, long)):
      raise TypeError('initial_value must be a number or None; received %r' %
                      initial_value)
    if namespace is None:
      namespace = namespace_manager.get_namespace()
    return self.memcache_off_batcher.add((key, delta),
                                          (initial_value, namespace, deadline))

  def memcache_decr(self, key, delta=1, initial_value=None, namespace=None,
                    deadline=None):
    if not isinstance(key, basestring):
      raise TypeError('key must be a string; received %r' % key)
    if not isinstance(delta, (int, long)):
      raise TypeError('delta must be a number; received %r' % delta)
    if initial_value is not None and not isinstance(initial_value, (int, long)):
      raise TypeError('initial_value must be a number or None; received %r' %
                      initial_value)
    if namespace is None:
      namespace = namespace_manager.get_namespace()
    return self.memcache_off_batcher.add((key, -delta),
                                          (initial_value, namespace, deadline))
