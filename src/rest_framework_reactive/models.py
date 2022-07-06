from django.db import models

from .protocol import *


class Observer(models.Model):
    """State of an observer."""

    CHANGE_TYPES = (
        ORM_NOTIFY_KIND_CREATE,
        ORM_NOTIFY_KIND_UPDATE,
        ORM_NOTIFY_KIND_DELETE,
    )

    id = models.CharField(primary_key=True, max_length=64)

    # table of the observed resource
    table = models.CharField(max_length=100)
    # primary key of the observed resource (null if watching the whole table)
    resource = models.IntegerField(null=True)
    change_type = models.CharField(choices=CHANGE_TYPES)
    subscribers = models.ManyToManyField('Subscriber')

    def __str__(self):
        return 'id={id}'.format(id=self.id)

    class Meta:
        unique_together = ('table', 'resource_pk', 'change_type')


class Subscriber(models.Model):
    """Subscriber to an observer."""

    session_id = models.CharField(primary_key=True, max_length=100)
    created = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return 'session_id={session_id}'.format(session_id=self.session_id)
