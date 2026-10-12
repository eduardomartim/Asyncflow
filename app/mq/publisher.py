"""AsyncFlow RabbitMQ message publisher.

All publishes here target topology already declared by
``RabbitMQConnection._declare_topology`` (see app/mq/connection.py for the
exchange/queue/DLX wiring this relies on).
"""
import json
import logging
from typing import Any

import aio_pika
from aio_pika.abc import AbstractChannel

from app.core.constants import ExchangeName, MessageLabel, QueueName, RoutingKey

logger = logging.getLogger("asyncflow.mq")


class MessagePublisher:
    """Publishes task/retry/dead-letter messages to RabbitMQ."""

    def __init__(self, channel: AbstractChannel) -> None:
        self.channel = channel

    async def publish_task(
        self,
        task_id: str,
        task_type: str,
        payload: dict[str, Any],
        correlation_id: str | None = None,
    ) -> None:
        """Publish a task message onto the main "tasks" queue.

        Goes through the named ``tasks`` topic exchange (NOT the default
        exchange) with routing key "task", matching the binding declared in
        ``RabbitMQConnection``.
        """
        exchange = await self.channel.get_exchange(ExchangeName.tasks.value)
        body = json.dumps(
            {
                "task_id": task_id,
                "task_type": task_type,
                "payload": payload,
                "label": MessageLabel.task.value,
            }
        ).encode()

        await exchange.publish(
            aio_pika.Message(
                body=body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                correlation_id=correlation_id or task_id,
                headers={"task_id": task_id, "label": MessageLabel.task.value},
            ),
            routing_key=RoutingKey.task.value,
        )
        logger.info("task_published", extra={"task_id": task_id})

    async def publish_retry(
        self,
        task_id: str,
        task_type: str,
        payload: dict[str, Any],
        attempt: int,
        delay_seconds: float,
        error: str | None = None,
    ) -> None:
        """Place a task back onto the delay/retry queue.

        Published directly to the ``tasks.retry`` queue via the default
        exchange (routing key == queue name is how the default exchange
        delivers). The per-message ``expiration`` implements the actual
        backoff delay; the queue's DLX (see connection.py) bounces the
        message back onto "tasks" once that expiration elapses.

        NOTE: aio_pika's ``Message(expiration=...)`` takes SECONDS and does
        its own seconds->milliseconds conversion internally -- do not also
        multiply by 1000 here, or the real delay ends up ~1000x too long.
        """
        body = json.dumps(
            {
                "task_id": task_id,
                "task_type": task_type,
                "payload": payload,
                "attempt": attempt,
                "label": MessageLabel.task.value,
            }
        ).encode()

        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                correlation_id=task_id,
                expiration=delay_seconds,
                headers={
                    "task_id": task_id,
                    "attempt": attempt,
                    "label": MessageLabel.task.value,
                    "last_error": (error or "")[:500],
                },
            ),
            routing_key=QueueName.tasks_retry.value,
        )
        logger.info(
            "task_retry_scheduled",
            extra={"task_id": task_id, "attempt": attempt, "delay_seconds": delay_seconds},
        )

    async def publish_dead_letter(
        self,
        task_id: str,
        task_type: str,
        reason: str,
    ) -> None:
        """Publish a terminal dead-letter notice for a task that exhausted retries."""
        exchange = await self.channel.get_exchange(ExchangeName.dead_letter.value)
        body = json.dumps(
            {
                "task_id": task_id,
                "task_type": task_type,
                "reason": reason,
                "label": MessageLabel.dead_letter.value,
            }
        ).encode()

        await exchange.publish(
            aio_pika.Message(
                body=body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                correlation_id=task_id,
                headers={"task_id": task_id, "label": MessageLabel.dead_letter.value},
            ),
            routing_key=RoutingKey.dead_letter.value,
        )
        logger.warning("task_dead_lettered", extra={"task_id": task_id, "reason": reason})
