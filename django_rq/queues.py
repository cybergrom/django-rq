from collections import Counter
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

import redis
from rq.queue import FailedQueue, Queue

from django_rq import thread_queue


def get_commit_mode():
    """
    Disabling AUTOCOMMIT causes enqueued jobs to be stored in a temporary queue.
    Jobs in this queue are only enqueued after the request is completed and are
    discarded if the request causes an exception (similar to db transactions).

    To disable autocommit, put this in settings.py:
    RQ = {
        'AUTOCOMMIT': False,
    }
    """
    RQ = getattr(settings, 'RQ', {})
    return RQ.get('AUTOCOMMIT', True)


class DjangoFailedRQ(FailedQueue):
    def __init__(self, *args, **kwargs):
        from .settings import CONNECTIONS
        self.connection_name = kwargs.pop('connection_name')
        kwargs['connection'] = get_redis_connection(CONNECTIONS[self.connection_name])
        super(DjangoFailedRQ, self).__init__(*args, **kwargs)

    @property
    def jobs_count(self):
        return len(self.jobs)


class DjangoRQ(Queue):
    """
    A subclass of RQ's QUEUE that allows jobs to be stored temporarily to be
    enqueued later at the end of Django's request/response cycle.
    """

    def __init__(self, *args, **kwargs):
        from .settings import CONNECTIONS
        self.connection_name = kwargs.pop('connection_name', None)
        kwargs['connection'] = get_redis_connection(CONNECTIONS[self.connection_name])
        autocommit = kwargs.pop('autocommit', True)
        self._autocommit = get_commit_mode() if autocommit is None else autocommit
        return super(DjangoRQ, self).__init__(*args, **kwargs)

    def original_enqueue_call(self, *args, **kwargs):
        return super(DjangoRQ, self).enqueue_call(*args, **kwargs)

    def enqueue_call(self, *args, **kwargs):
        # print args, kwargs
        if self._autocommit:
            return self.original_enqueue_call(*args, **kwargs)
        else:
            thread_queue.add(self, args, kwargs)

    @property
    def jobs_count(self):
        return len(self.jobs)


def get_redis_connection(config):
    """
    Returns a redis connection from a connection config
    """
    if 'URL' in config:
        return redis.from_url(config['URL'], db=config['DB'])
    if 'USE_REDIS_CACHE' in config.keys():

        from django.core.cache import get_cache
        cache = get_cache(config['USE_REDIS_CACHE'])

        if hasattr(cache, 'client'):
            # We're using django-redis. The cache's `client` attribute
            # is a pluggable backend that return its Redis connection as
            # its `client`
            try:
                return cache.client.client
            except NotImplementedError:
                pass
        else:
            # We're using django-redis-cache
            return cache._client

    return redis.Redis(host=config['HOST'],
                       port=config['PORT'], db=config['DB'],
                       password=config.get('PASSWORD', None))


def get_connection(name='default'):
    """
    Returns a Redis connection to use based on parameters in settings.RQ_QUEUES
    """
    from .settings import CONNECTIONS
    return get_redis_connection(CONNECTIONS[name])



def get_queue(name='default', default_timeout=None, async=None,
              autocommit=None, connection_name='default'):
    """
    Returns an rq Queue using parameters defined in ``RQ_QUEUES``
    """
    from .settings import CONNECTIONS

    # If async is provided, use it, otherwise, get it from the configuration
    if async is None:
        async = CONNECTIONS[connection_name].get('ASYNC', True)

    return DjangoRQ(name, default_timeout=default_timeout,
                    connection_name=connection_name, async=async,
                    autocommit=autocommit)


def get_connection_queue_names(connection):
    return Counter([q.split(":")[-1].split(".")[0]
                    for q in connection.keys("rq:worker:*")])


def get_queues(*queue_names, **kwargs):
    """
    Return queue instances from specified queue names.
    All instances must use the same Redis connection.
    """
    from .settings import CONNECTIONS
    autocommit = kwargs.get('autocommit', None)
    if len(queue_names) == 0:
        return [get_queue(autocommit=autocommit)]
    else:
        queues = []
        connections = set()

        for name in queue_names:
            if "." in name:
                connection_name, name = name.split(".")
            else:
                connection_name = 'default'
            if not connection_name in CONNECTIONS:
                raise ValueError('Unknown connection %s.' % connection_name)
            connections.add(connection_name)
            queues.append(get_queue(name, autocommit=autocommit, connection_name=connection_name))

        if len(connections) > 1:
            raise ValueError('Queues must have the same redis connection.')

        return queues


def enqueue(func, *args, **kwargs):
    """
    A convenience function to put a job in the default queue. Usage::

    from django_rq import enqueue
    enqueue(func, *args, **kwargs)
    """
    return get_queue().enqueue(func, *args, **kwargs)



"""
If rq_scheduler is installed, provide a ``get_scheduler`` function that
behaves like ``get_connection``, except that it returns a ``Scheduler``
instance instead of a ``Queue`` instance.
"""
try:
    from rq_scheduler import Scheduler

    class DjangoScheduler(Scheduler):
        def __init__(self, *args, **kwargs):
            from .settings import CONNECTIONS
            self.connection_name = kwargs.pop('connection_name')
            kwargs['connection'] = get_redis_connection(CONNECTIONS[self.connection_name])
            super(DjangoScheduler, self).__init__(*args, **kwargs)

    def get_scheduler(connection_name='default', name='default', interval=60):
        """
        Returns an RQ Scheduler instance using parameters defined in
        ``RQ_QUEUES``
        """
        return DjangoScheduler(name, interval=interval, connection_name=connection_name)
except ImportError:
    def get_scheduler(*args, **kwargs):
        raise ImproperlyConfigured('rq_scheduler not installed')
