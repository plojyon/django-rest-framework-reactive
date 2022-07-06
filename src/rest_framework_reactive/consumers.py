import asyncio
import collections
import pickle

from django.db.models import Q
from channels.consumer import AsyncConsumer
from channels.db import database_sync_to_async
from channels.generic.websocket import JsonWebsocketConsumer
from django.core.cache import cache

from .connection import get_queryobserver_settings
from .models import Observer, Subscriber
from .observer import QueryObserver
from .protocol import *


class MainConsumer(AsyncConsumer):
    """Consumer for polling observers."""

    async def observer_orm_notify(self, message):
        """Process notification from ORM."""

        @database_sync_to_async
        def get_subscribers(table, item, kind):
            """Find all subscribers watching a given item in a table."""
            query = Q(observers__table=table, observers__change_type=kind)
            query &= Q(observers__resource=item) | Q(observers__resource__isnull=True)
            return list(Subscriber.objects.filter(query))

        table = message['table']
        item = message['primary_key']
        kind = message['kind']

        subscribers = await get_subscribers(table, item, kind)

        for session_id in subscribers:
            await self.channel_layer.send(
                GROUP_SESSIONS.format(session_id=session_id),
                {'type': TYPE_ITEM_UPDATE, 'table': table, 'item': item, 'kind': kind},
            )


class ClientConsumer(JsonWebsocketConsumer):
    """Client consumer."""

    def websocket_connect(self, message):
        """Called when WebSocket connection is established."""
        self.session_id = self.scope['url_route']['kwargs']['subscriber_id']
        super().websocket_connect(message)

        # Create new subscriber object.
        Subscriber.objects.get_or_create(session_id=self.session_id)

    @property
    def groups(self):
        """Groups this channel should add itself to."""
        if not hasattr(self, 'session_id'):
            return []

        return [GROUP_SESSIONS.format(session_id=self.session_id)]

    def disconnect(self, code):
        """Called when WebSocket connection is closed."""
        Subscriber.objects.filter(session_id=self.session_id).delete()

    def observer_update(self, message):
        """Called when update is received."""
        self.send_json(
            {
                'table': message['table'],
                'item': message['item'],
                'kind': message['kind'],
            }
        )
