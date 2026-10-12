"""RabbitMQ module for AsyncFlow - connection and topology management.

Topology (all durable):

    tasks_exchange (topic) --rk "task"--> queue "tasks"
                                              |  worker consumes here
                                              |  on transient failure, worker
                                              v  republishes to "tasks.retry"
    queue "tasks.retry" (per-message TTL = backoff delay)
       x-dead-letter-exchange -> tasks_exchange, rk "task"
       (so once a message's TTL elapses, RabbitMQ itself redelivers it into
        "tasks" -- this is the "TTL + DLX as a delay queue" pattern)

    dead_letter_exchange (topic) --rk "dead_letter"--> queue "tasks.dead_letter"
       (terminal queue; worker publishes here once attempts are exhausted.
        Also wired as the *queue-level* DLX for "tasks" itself, so any
        message rejected by a bug we didn't anticipate -- rather than our
        own retry/DLQ bookkeeping -- still lands somewhere inspectable
        instead of being silently dropped.)
"""
import logging

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractQueue, AbstractRobustConnection

from app.core.config import settings
from app.core.constants import ExchangeName, QueueName, RoutingKey

logger = logging.getLogger("asyncflow.mq")

# Safety valve: cap queue depth so a stuck/absent consumer can't grow the
# retry/tasks queues without bound (requirement: no unbounded queue growth).
_MAX_QUEUE_LENGTH = 10_000
_MAX_RETRY_QUEUE_LENGTH = 2_000

# RabbitMQ rejects a queue.declare whose arguments don't exactly match an
# already-declared queue (PRECONDITION_FAILED). The worker declares "tasks"
# too (to get a Queue handle to consume from), so it MUST reuse these exact
# arguments rather than its own -- hence these are module-level constants,
# not inlined into _declare_topology below.
TASKS_QUEUE_ARGUMENTS = {
    "x-dead-letter-exchange": ExchangeName.dead_letter.value,
    "x-dead-letter-routing-key": RoutingKey.dead_letter.value,
    "x-max-length": _MAX_QUEUE_LENGTH,
}


class RabbitMQConnection:
    """Manages the RabbitMQ connection, channel, and declared topology."""

    def __init__(self) -> None:
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None

    @property
    def connection(self) -> AbstractRobustConnection:
        if self._connection is None:
            raise RuntimeError("RabbitMQ connection not established")
        return self._connection

    @property
    def channel(self) -> AbstractChannel:
        if self._channel is None:
            raise RuntimeError("RabbitMQ channel not created")
        return self._channel

    @property
    def is_connected(self) -> bool:
        return self._connection is not None and not self._connection.is_closed

    async def connect(self) -> None:
        """Establish the RabbitMQ connection, open a channel, declare topology."""
        self._connection = await aio_pika.connect_robust(settings.rabbitmq_dsn)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=settings.worker_concurrency)
        await self._declare_topology()
        logger.info("rabbitmq_connected", extra={"host": settings.rabbitmq_host})

    async def close(self) -> None:
        """Close the RabbitMQ connection gracefully."""
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
        self._connection = None
        self._channel = None

    async def _declare_topology(self) -> None:
        channel = self.channel

        tasks_exchange: AbstractExchange = await channel.declare_exchange(
            ExchangeName.tasks.value, aio_pika.ExchangeType.TOPIC, durable=True
        )
        dead_letter_exchange: AbstractExchange = await channel.declare_exchange(
            ExchangeName.dead_letter.value, aio_pika.ExchangeType.TOPIC, durable=True
        )

        dlq: AbstractQueue = await channel.declare_queue(
            QueueName.tasks_dead_letter.value,
            durable=True,
            arguments={"x-max-length": _MAX_QUEUE_LENGTH},
        )
        await dlq.bind(dead_letter_exchange, routing_key=RoutingKey.dead_letter.value)

        tasks_queue: AbstractQueue = await channel.declare_queue(
            QueueName.tasks.value,
            durable=True,
            arguments=TASKS_QUEUE_ARGUMENTS,
        )
        await tasks_queue.bind(tasks_exchange, routing_key=RoutingKey.task.value)

        # No queue-level x-message-ttl here on purpose: the worker sets a
        # per-message `expiration` (exponential backoff) when it republishes
        # a failed task, so different attempts can wait different amounts
        # of time. Once a message's own TTL elapses, its DLX below bounces
        # it back into tasks_exchange/"task" -- i.e. back onto "tasks".
        retry_queue: AbstractQueue = await channel.declare_queue(
            QueueName.tasks_retry.value,
            durable=True,
            arguments={
                "x-dead-letter-exchange": ExchangeName.tasks.value,
                "x-dead-letter-routing-key": RoutingKey.task.value,
                "x-max-length": _MAX_RETRY_QUEUE_LENGTH,
            },
        )
        # The retry queue is never consumed directly nor routed to by a
        # routing key -- messages are placed on it by publishing with
        # routing_key=tasks_retry's name against the default exchange (see
        # MessagePublisher.publish_retry). No exchange binding needed.
        del retry_queue
