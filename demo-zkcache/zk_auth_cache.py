from contextlib import contextmanager
import logging
import pickle

from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, NodeExistsError, NoNodeError
from libcloud.common.openstack_identity import OpenStackAuthenticationCache

LOG = logging.getLogger(__name__)

@contextmanager
def connect(zk):
    zk.start()
    try:
        yield zk
    finally:
        zk.stop()
        zk.close()


class ZookeeperAuthenticationCache(OpenStackAuthenticationCache):
    def __init__(self, zookeeper=None, path=None):
        self._zk = zookeeper or KazooClient(hosts='127.0.0.1:2181')
        self._node_versions = {}  # key: version (int)
        self.root_path = path or '/openstack-tokens'

    def _key_path(self, key):
        return '%s/%s' % (
            self.root_path,
            '/'.join(map(lambda k: str(k).replace('/', ''), key)))

    def get(self, key):
        path = self._key_path(key)
        LOG.debug('Getting %s from cache', path)
        try:
            with connect(self._zk):
                value, stat = self._zk.get(path)
        except NoNodeError:
            return None

        self._node_versions[key] = stat.version
        return pickle.loads(value)

    def put(self, key, context):
        path = self._key_path(key)
        LOG.debug('Caching %s', path)
        context_bytes = pickle.dumps(context)
        with connect(self._zk):
            try:
                self._zk.create(path, value=context_bytes, makepath=True)
            except NodeExistsError:
                self._zk.set(path, context_bytes)  # TODO version?

    def clear(self, key):
        # We have not fetched this key before, so we do not know which version
        # to void.
        # if key not in self._node_versions:
        #     return

        LOG.debug('Clearing %s', key)
        try:
            with connect(self._zk):
                self._zk.delete(self._key_path(key), version=self._node_versions[key])
        except (BadVersionError, NoNodeError):
            pass
