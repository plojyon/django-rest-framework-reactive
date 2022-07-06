import asyncio
import collections
import pickle

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
        def get_observers(table):
            """Find all observers with dependencies on the given table."""

            return list(
                Observer.objects.filter(
                    dependencies__table=table, subscribers__isnull=False
                )
                .distinct('pk')
                .values_list('pk', flat=True)
            )

        observers_ids = await get_observers(message['table'])

        for observer_id in observers_ids:
            await self.channel_layer.send(
                CHANNEL_WORKER, {'type': TYPE_EVALUATE, 'observer': observer_id}
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
        """Called when update from observer is received."""
        # Demultiplex observer update into multiple messages.
        for action in ('added', 'changed', 'removed'):
            for item in message[action]:
                self.send_json(
                    {
                        'msg': action,
                        'observer': message['observer'],
                        'primary_key': message['primary_key'],
                        'order': item['order'],
                        'item': item['data'],
                    }
                )
