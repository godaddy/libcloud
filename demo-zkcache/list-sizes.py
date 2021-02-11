#!/usr/bin/env python
import logging
import os

from kazoo.client import KazooClient
from libcloud.compute.providers import get_driver

from zk_auth_cache import ZookeeperAuthenticationCache

logging.basicConfig(level=logging.DEBUG)


if __name__ == '__main__':
    dcls = get_driver('OPENSTACK')
    zk = KazooClient(hosts='127.0.0.1:2181')
    cache = ZookeeperAuthenticationCache(zookeeper=zk)
    # cache = None
    url = os.getenv('OS_AUTH_URL').rsplit('/', 1)[0]
    driver = dcls(
        os.getenv('OS_USERNAME'),
        os.getenv('OS_PASSWORD'),
        ex_force_auth_url=url,
        ex_force_auth_version='3.x_password',
        ex_force_service_region=os.getenv('OS_SERVICE_REGION'),
        ex_tenant_name=os.getenv('OS_TENANT_NAME'),
        ex_auth_cache=cache)
    print(driver.list_sizes())
    print(driver.list_sizes())
