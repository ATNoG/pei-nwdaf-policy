"""
Kafka Interceptor - Consumer-side policy enforcement for Kafka messages.
"""
import json
import logging
import asyncio
from typing import Optional, Any, Iterator

from confluent_kafka import Consumer, Message, KafkaError

from .client import PolicyClient

logger = logging.getLogger(__name__)


class KafkaPolicyInterceptor:
    """
    Wraps a Kafka Consumer to apply policy enforcement on messages.

    Messages that are denied by policy are silently skipped.
    Messages that pass policy are optionally transformed.
    """

    def __init__(
        self,
        consumer: Consumer,
        policy_client: PolicyClient,
        component_id: str,
        source_component: str,
        enable_policy: bool = False
    ):
        """
        Initialize the Kafka Policy Interceptor.

        Args:
            consumer: Confluent Kafka Consumer instance
            policy_client: Policy Client instance
            component_id: This component's ID (sink)
            source_component: Source component ID (message producer)
            enable_policy: Whether to enforce policy (non-blocking if False)
        """
        self.consumer = consumer
        self.policy_client = policy_client
        self.component_id = component_id
        self.source_component = source_component
        self.enable_policy = enable_policy

        self._messagesDenied = 0
        self._messagesAllowed = 0

        logger.info(
            f"KafkaPolicyInterceptor initialized: "
            f"component={component_id}, source={source_component}, enabled={enable_policy}"
        )

    def _replace_message(self, original: Message, new_data: dict) -> Message:
        """
        Create a new message with transformed data.

        Args:
            original: Original Kafka message
            new_data: New data to replace message value

        Returns:
            Modified message (Note: this creates a wrapper, not a true Kafka Message)
        """
        class PolicyMessage:
            """Wrapper for Kafka message after policy transformation."""

            def __init__(self, original: Message, data: dict):
                self._original = original
                self._data = data

            def value(self) -> bytes:
                return json.dumps(self._data).encode('utf-8')

            def key(self):
                return self._original.key()

            def topic(self) -> str:
                return self._original.topic()

            def partition(self) -> int:
                return self._original.partition()

            def offset(self) -> int:
                return self._original.offset()

            def error(self) -> Optional[KafkaError]:
                return self._original.error()

            def headers(self):
                return self._original.headers()

            def timestamp(self):
                return self._original.timestamp()

        return PolicyMessage(original, new_data)

    def __iter__(self) -> Iterator[Message]:
        """
        Iterate over messages, applying policy enforcement.

        Messages that fail policy checks are skipped.
        Messages that pass are optionally transformed.

        Yields:
            Kafka Message objects (original or transformed)
        """
        for raw_message in self.consumer:
            try:
                # Decode message value
                if raw_message.value() is None:
                    yield raw_message
                    continue

                try:
                    data = json.loads(raw_message.value().decode('utf-8'))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    # Not JSON, pass through as-is
                    yield raw_message
                    continue

                # Apply policy if enabled
                if self.enable_policy:
                    # Check + Transform in one call
                    result = asyncio.run(self.policy_client.process_data(
                        source_id=self.source_component,
                        sink_id=self.component_id,
                        data=data
                    ))

                    if result.allowed:
                        self._messagesAllowed += 1
                        # Return transformed message
                        yield self._replace_message(raw_message, result.data)
                    else:
                        # Skip denied message
                        self._messagesDenied += 1
                        logger.debug(
                            f"Message denied by policy: {result.reason}"
                        )
                        continue
                else:
                    # Policyless mode - pass through
                    self._messagesAllowed += 1
                    yield raw_message

            except Exception as e:
                # Fail-open - policy errors don't block message flow
                logger.warning(f"Policy check failed: {e}, passing data through")
                self._messagesAllowed += 1
                yield raw_message

    def poll(self, timeout: float = -1) -> Optional[Message]:
        """
        Poll for a single message with policy enforcement.

        Args:
            timeout: Poll timeout in seconds

        Returns:
            Message or None
        """
        # Use iterator to get next message with policy applied
        try:
            return next(iter(self))
        except StopIteration:
            return None

    def get_stats(self) -> dict:
        """Get interceptor statistics."""
        return {
            "messages_allowed": self._messagesAllowed,
            "messages_denied": self._messagesDenied,
            "total_messages": self._messagesAllowed + self._messagesDenied
        }


def create_policy_consumer(
    bootstrap_servers: str,
    group_id: str,
    topics: list[str],
    policy_service_url: str,
    component_id: str,
    source_component: str,
    consumer_config: Optional[dict] = None,
    enable_policy: bool = False
) -> KafkaPolicyInterceptor:
    """
    Convenience function to create a policy-enabled Kafka consumer.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        group_id: Consumer group ID
        topics: List of topics to subscribe to
        policy_service_url: URL of Policy Service
        component_id: This component's ID
        source_component: Source component ID
        consumer_config: Additional Kafka consumer configuration
        enable_policy: Whether to enable policy enforcement

    Returns:
        KafkaPolicyInterceptor instance
    """
    # Default consumer config
    config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    }
    if consumer_config:
        config.update(consumer_config)

    # Create consumer
    consumer = Consumer(config)
    consumer.subscribe(topics)

    # Create policy client
    policy_client = PolicyClient(
        service_url=policy_service_url,
        component_id=component_id,
        enable_policy=enable_policy
    )

    # Create and return interceptor
    return KafkaPolicyInterceptor(
        consumer=consumer,
        policy_client=policy_client,
        component_id=component_id,
        source_component=source_component,
        enable_policy=enable_policy
    )
