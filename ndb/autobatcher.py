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
from . import eventloop
from . import utils
from . import tasklets

__all__ = ['AutoBatcher']

class AutoBatcher(object):
  """Batches multiple async calls if they share the same rpc options.

  Here is an example to explain what this class does.

  Life of a key.get_async(options) API call:
  *) Key gets the singleton Context instance and invokes Context.get.
  *) Context.get calls Context._get_batcher.add(key, options). This
     returns a future "fut" as the return value of key.get_async.
     At this moment, key.get_async returns.

  *) When more than "limit" number of _get_batcher.add() was called,
     _get_batcher invokes its self._todo_tasklet, Context._get_tasklet,
     with the list of keys seen so far.
  *) Context._get_tasklet fires a MultiRPC and waits on it.
  *) Upon MultiRPC completion, Context._get_tasklet passes on the results
     to the respective "fut" from key.get_async.

  *) If user calls "fut".get_result() before "limit" number of add() was called,
     "fut".get_result() will repeatedly call eventloop.run1().
  *) After processing immediate callbacks, eventloop will run idlers.
     AutoBatcher._on_idle is an idler.
  *) _on_idle will run the "todo_tasklet" before the batch is full.

  So the engine is todo_tasklet, which is a proxy tasklet that can combine
  arguments into batches and passes along results back to respective futures.
  This class is mainly a helper that invokes todo_tasklet with the right
  arguments at the right time.
  """

  def __init__(self, todo_tasklet, limit):
    """Init.

    Args:
      todo_tasklet: the tasklet that actually fires RPC and waits on a MultiRPC.
        It should take a list of (future, arg) pairs and an "options" as
        arguments. "options" are rpc options.
      limit: max number of items to batch for each distinct value of "options".
    """
    self._todo_tasklet = todo_tasklet
    self._limit = limit
    # A map from "options" to a list of (future, arg) tuple.
    # future is the future return from a single async operations.
    self._queues = {}
    self._running = []  # A list of in-flight todo_tasklet futures.
    self._cache = {}  # Cache of in-flight todo_tasklet futures.

  def __repr__(self):
    return '%s(%s)' % (self.__class__.__name__, self._todo_tasklet.__name__)

  def run_queue(self, options, todo):
    """Actually run the _todo_tasklet."""
    utils.logging_debug('AutoBatcher(%s): %d items',
                        self._todo_tasklet.__name__, len(todo))
    batch_fut = self._todo_tasklet(todo, options)
    self._running.append(batch_fut)
    # Add a callback when we're done.
    batch_fut.add_callback(self._finished_callback, batch_fut, todo)

  def _on_idle(self):
    """An idler eventloop can run.

    Eventloop calls this when it has finished processing all immediate
    callbacks. This method runs _todo_tasklet even before the batch is full.
    """
    if not self.action():
      return None
    return True

  def add(self, arg, options=None):
    """Adds an arg and gets back a future.

    Args:
      arg: one argument for _todo_tasklet.
      options: rpc options.

    Return:
      An instance of future, representing the result of running
        _todo_tasklet without batching.
    """
    fut = tasklets.Future('%s.add(%s, %s)' % (self, arg, options))
    todo = self._queues.get(options)
    if todo is None:
      utils.logging_debug('AutoBatcher(%s): creating new queue for %r',
                          self._todo_tasklet.__name__, options)
      if not self._queues:
        eventloop.add_idle(self._on_idle)
      todo = self._queues[options] = []
    todo.append((fut, arg))
    if len(todo) >= self._limit:
      del self._queues[options]
      self.run_queue(options, todo)
    return fut

  def add_once(self, arg, options=None):
    cache_key = (arg, options)
    fut = self._cache.get(cache_key)
    if fut is None:
      fut = self.add(arg, options)
      self._cache[cache_key] = fut
      fut.add_immediate_callback(self._cache.__delitem__, cache_key)
    return fut

  def action(self):
    queues = self._queues
    if not queues:
      return False
    options, todo = queues.popitem()  # TODO: Should this use FIFO ordering?
    self.run_queue(options, todo)
    return True

  def _finished_callback(self, batch_fut, todo):
    """Passes exception along.

    Args:
      batch_fut: the batch future returned by running todo_tasklet.
      todo: (fut, option) pair. fut is the future return by each add() call.

    If the batch fut was successful, it has already called fut.set_result()
    on other individual futs. This method only handles when the batch fut
    encountered an exception.
    """
    self._running.remove(batch_fut)
    err = batch_fut.get_exception()
    if err is not None:
      tb = batch_fut.get_traceback()
      for (fut, _) in todo:
        if not fut.done():
          fut.set_exception(err, tb)

  @tasklets.tasklet
  def flush(self):
    while self._running or self.action():
      if self._running:
        yield self._running  # A list of Futures