from django.conf import settings
from django.core.exceptions import ImproperlyConfigured


SHOW_ADMIN_LINK = getattr(settings, 'RQ_SHOW_ADMIN_LINK', False)

CONNECTIONS = getattr(settings, 'RQ_CONNECTIONS', None)
if CONNECTIONS is None:
    raise ImproperlyConfigured("You have to define RQ_CONNECTIONS in settings.py")
NAME = getattr(settings, 'RQ_NAME', 'default')
BURST = getattr(settings, 'RQ_BURST', False)
