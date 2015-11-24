import atexit
import code
import readline
import os
import sys

import ndb
from ndb import tasklets
from ndb.google_imports import datastore_pbs

HISTORY_PATH = os.path.expanduser('~/.ndb_shell_history')

def shell():

  if (not os.environ.get('DATASTORE_APP_ID', None)
      and not os.environ.get('DATASTORE_PROJECT_ID', None)):
    raise ValueError('Must set either DATASTORE_APP_ID or DATASTORE_PROJECT_ID'
                     ' environment variables.')

  ndb.get_context().set_memcache_policy(False)
  ndb.get_context().set_cache_policy(False)

  # ndb will set the application ID.
  application_id = os.environ['APPLICATION_ID']
  id_resolver = datastore_pbs.IdResolver((application_id,))
  project_id = id_resolver.resolve_project_id(application_id)

  banner = """ndb shell
  Python %s
  Project: %s
  The ndb module is already imported.
  """ % (sys.version, project_id)

  imports = {
    'ndb': ndb,
  }

  # set up the environment
  os.environ['SERVER_SOFTWARE'] = 'Development (ndb_shell)/0.1'

  sys.ps1 = '%s> ' % project_id
  if readline is not None:
    # set up readline
    readline.parse_and_bind('tab: complete')
    atexit.register(lambda: readline.write_history_file(HISTORY_PATH))
    if os.path.exists(HISTORY_PATH):
      readline.read_history_file(HISTORY_PATH)

  code.interact(banner=banner, local=imports)

if __name__ == '__main__':
  shell()