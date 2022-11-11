import os

from django.test import TestCase
from redis import Redis

from taskkit.impl.redis import RedisBackend
from taskkit.impl.django import DjangoBackend

from .test_backend import BackendTests


redis = Redis(host=os.environ.get('REDIS_HOST', '127.0.0.1'),
              port=int(os.environ.get('REDIS_PORT', '6379')))


class RedisBackendTests(BackendTests):
    backend = RedisBackend(redis)


class DjangoBackendTests(TestCase, BackendTests):
    backend = DjangoBackend()
