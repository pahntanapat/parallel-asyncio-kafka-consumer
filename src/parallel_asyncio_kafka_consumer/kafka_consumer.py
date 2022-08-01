import asyncio
from concurrent.futures import Future, ThreadPoolExecutor, wait
from ctypes import Union, c_bool
from multiprocessing.sharedctypes import Value
from typing import Any, Awaitable, Callable, Tuple
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, TopicPartition, __version__
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor


class AIOParallelKafkaConsumer:
    """
    A client that consumes records from a Kafka cluster.

    The consumer will transparently handle the failure of servers in the Kafka
    cluster, and adapt as topic-partitions are created or migrate between
    brokers. It also interacts with the assigned kafka Group Coordinator node
    to allow multiple consumers to load balance consumption of topics (feature
    of kafka >= 0.9.0.0).

    .. _create_connection:
        https://docs.python.org/3/library/asyncio-eventloop.html\
        #creating-connections

    Arguments:
        *topics (str): optional list of topics to subscribe to. If not set,
            call subscribe() or assign() before consuming records. Passing
            topics directly is same as calling ``subscribe()`` API.
        bootstrap_servers: 'host[:port]' string (or list of 'host[:port]'
            strings) that the consumer should contact to bootstrap initial
            cluster metadata. This does not have to be the full node list.
            It just needs to have at least one broker that will respond to a
            Metadata API Request. Default port is 9092. If no servers are
            specified, will default to localhost:9092.
        client_id (str): a name for this client. This string is passed in
            each request to servers and can be used to identify specific
            server-side log entries that correspond to this client. Also
            submitted to GroupCoordinator for logging with respect to
            consumer group administration. Default: 'aiokafka-{version}'
        group_id (str or None): name of the consumer group to join for dynamic
            partition assignment (if enabled), and to use for fetching and
            committing offsets. If None, auto-partition assignment (via
            group coordinator) and offset commits are disabled.
            Default: None
        key_deserializer (callable): Any callable that takes a
            raw message key and returns a deserialized key.
        value_deserializer (callable, optional): Any callable that takes a
            raw message value and returns a deserialized value.
        fetch_min_bytes (int): Minimum amount of data the server should
            return for a fetch request, otherwise wait up to
            fetch_max_wait_ms for more data to accumulate. Default: 1.
        fetch_max_bytes (int): The maximum amount of data the server should
            return for a fetch request. This is not an absolute maximum, if
            the first message in the first non-empty partition of the fetch
            is larger than this value, the message will still be returned
            to ensure that the consumer can make progress. NOTE: consumer
            performs fetches to multiple brokers in parallel so memory
            usage will depend on the number of brokers containing
            partitions for the topic.
            Supported Kafka version >= 0.10.1.0. Default: 52428800 (50 Mb).
        fetch_max_wait_ms (int): The maximum amount of time in milliseconds
            the server will block before answering the fetch request if
            there isn't sufficient data to immediately satisfy the
            requirement given by fetch_min_bytes. Default: 500.
        max_partition_fetch_bytes (int): The maximum amount of data
            per-partition the server will return. The maximum total memory
            used for a request = #partitions * max_partition_fetch_bytes.
            This size must be at least as large as the maximum message size
            the server allows or else it is possible for the producer to
            send messages larger than the consumer can fetch. If that
            happens, the consumer can get stuck trying to fetch a large
            message on a certain partition. Default: 1048576.
        max_poll_records (int): The maximum number of records returned in a
            single call to ``getmany()``. Defaults ``None``, no limit.
        request_timeout_ms (int): Client request timeout in milliseconds.
            Default: 40000.
        retry_backoff_ms (int): Milliseconds to backoff when retrying on
            errors. Default: 100.
        auto_offset_reset (str): A policy for resetting offsets on
            OffsetOutOfRange errors: 'earliest' will move to the oldest
            available message, 'latest' will move to the most recent, and
            'none' will raise an exception so you can handle this case.
            Default: 'latest'.
        enable_auto_commit (bool): If true the consumer's offset will be
            periodically committed in the background. Default: True.
        auto_commit_interval_ms (int): milliseconds between automatic
            offset commits, if enable_auto_commit is True. Default: 5000.
        check_crcs (bool): Automatically check the CRC32 of the records
            consumed. This ensures no on-the-wire or on-disk corruption to
            the messages occurred. This check adds some overhead, so it may
            be disabled in cases seeking extreme performance. Default: True
        metadata_max_age_ms (int): The period of time in milliseconds after
            which we force a refresh of metadata even if we haven't seen any
            partition leadership changes to proactively discover any new
            brokers or partitions. Default: 300000
        partition_assignment_strategy (list): List of objects to use to
            distribute partition ownership amongst consumer instances when
            group management is used. This preference is implicit in the order
            of the strategies in the list. When assignment strategy changes:
            to support a change to the assignment strategy, new versions must
            enable support both for the old assignment strategy and the new
            one. The coordinator will choose the old assignment strategy until
            all members have been updated. Then it will choose the new
            strategy. Default: [RoundRobinPartitionAssignor]

        max_poll_interval_ms (int): Maximum allowed time between calls to
            consume messages (e.g., ``consumer.getmany()``). If this interval
            is exceeded the consumer is considered failed and the group will
            rebalance in order to reassign the partitions to another consumer
            group member. If API methods block waiting for messages, that time
            does not count against this timeout. See KIP-62 for more
            information. Default 300000
        rebalance_timeout_ms (int): The maximum time server will wait for this
            consumer to rejoin the group in a case of rebalance. In Java client
            this behaviour is bound to `max.poll.interval.ms` configuration,
            but as ``aiokafka`` will rejoin the group in the background, we
            decouple this setting to allow finer tuning by users that use
            ConsumerRebalanceListener to delay rebalacing. Defaults
            to ``session_timeout_ms``
        session_timeout_ms (int): Client group session and failure detection
            timeout. The consumer sends periodic heartbeats
            (heartbeat.interval.ms) to indicate its liveness to the broker.
            If no hearts are received by the broker for a group member within
            the session timeout, the broker will remove the consumer from the
            group and trigger a rebalance. The allowed range is configured with
            the **broker** configuration properties
            `group.min.session.timeout.ms` and `group.max.session.timeout.ms`.
            Default: 10000
        heartbeat_interval_ms (int): The expected time in milliseconds
            between heartbeats to the consumer coordinator when using
            Kafka's group management feature. Heartbeats are used to ensure
            that the consumer's session stays active and to facilitate
            rebalancing when new consumers join or leave the group. The
            value must be set lower than session_timeout_ms, but typically
            should be set no higher than 1/3 of that value. It can be
            adjusted even lower to control the expected time for normal
            rebalances. Default: 3000

        consumer_timeout_ms (int): maximum wait timeout for background fetching
            routine. Mostly defines how fast the system will see rebalance and
            request new data for new partitions. Default: 200
        api_version (str): specify which kafka API version to use.
            AIOKafkaConsumer supports Kafka API versions >=0.9 only.
            If set to 'auto', will attempt to infer the broker version by
            probing various APIs. Default: auto
        security_protocol (str): Protocol used to communicate with brokers.
            Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
            Default: PLAINTEXT.
        ssl_context (ssl.SSLContext): pre-configured SSLContext for wrapping
            socket connections. Directly passed into asyncio's
            `create_connection`_. For more information see :ref:`ssl_auth`.
            Default: None.
        exclude_internal_topics (bool): Whether records from internal topics
            (such as offsets) should be exposed to the consumer. If set to True
            the only way to receive records from an internal topic is
            subscribing to it. Requires 0.10+ Default: True
        connections_max_idle_ms (int): Close idle connections after the number
            of milliseconds specified by this config. Specifying `None` will
            disable idle checks. Default: 540000 (9 minutes).
        isolation_level (str): Controls how to read messages written
            transactionally. If set to *read_committed*,
            ``consumer.getmany()``
            will only return transactional messages which have been committed.
            If set to *read_uncommitted* (the default), ``consumer.getmany()``
            will return all messages, even transactional messages which have
            been aborted.

            Non-transactional messages will be returned unconditionally in
            either mode.

            Messages will always be returned in offset order. Hence, in
            *read_committed* mode, ``consumer.getmany()`` will only return
            messages up to the last stable offset (LSO), which is the one less
            than the offset of the first open transaction. In particular any
            messages appearing after messages belonging to ongoing transactions
            will be withheld until the relevant transaction has been completed.
            As a result, *read_committed* consumers will not be able to read up
            to the high watermark when there are in flight transactions.
            Further, when in *read_committed* the seek_to_end method will
            return the LSO. See method docs below. Default: "read_uncommitted"

        sasl_mechanism (str): Authentication mechanism when security_protocol
            is configured for SASL_PLAINTEXT or SASL_SSL. Valid values are:
            PLAIN, GSSAPI, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER.
            Default: PLAIN
        sasl_plain_username (str): username for sasl PLAIN authentication.
            Default: None
        sasl_plain_password (str): password for sasl PLAIN authentication.
            Default: None
        sasl_oauth_token_provider (kafka.oauth.abstract.AbstractTokenProvider):
            OAuthBearer token provider instance. (See kafka.oauth.abstract).
            Default: None

    Note:
        Many configuration parameters are taken from Java Client:
        https://kafka.apache.org/documentation.html#newconsumerconfigs

    """

    def __init__(self,
                 invoke_function: Callable[..., Awaitable[bool]],
                 *topics,
                 heartbeat: c_bool,
                 loop=None,
                 bootstrap_servers='localhost',
                 client_id=None,
                 group_id=None,
                 key_deserializer=None,
                 value_deserializer=None,
                 fetch_max_wait_ms=500,
                 fetch_max_bytes=52428800,
                 fetch_min_bytes=1,
                 max_partition_fetch_bytes=1 * 1024 * 1024,
                 request_timeout_ms=40 * 1000,
                 retry_backoff_ms=100,
                 auto_offset_reset='latest',
                 enable_auto_commit=True,
                 auto_commit_interval_ms=5000,
                 check_crcs=True,
                 metadata_max_age_ms=5 * 60 * 1000,
                 partition_assignment_strategy=(RoundRobinPartitionAssignor, ),
                 max_poll_interval_ms=300000,
                 rebalance_timeout_ms=None,
                 session_timeout_ms=10000,
                 heartbeat_interval_ms=3000,
                 consumer_timeout_ms=200,
                 max_poll_records=None,
                 ssl_context=None,
                 security_protocol='PLAINTEXT',
                 api_version='auto',
                 exclude_internal_topics=True,
                 connections_max_idle_ms=540000,
                 isolation_level="read_uncommitted",
                 sasl_mechanism="PLAIN",
                 sasl_plain_password=None,
                 sasl_plain_username=None,
                 sasl_kerberos_service_name='kafka',
                 sasl_kerberos_domain_name=None,
                 sasl_oauth_token_provider=None) -> None:

        self.heartbeat = heartbeat
        self.invoke_function = invoke_function
        if client_id is None:
            client_id = uuid4()

        self.consumer = AIOKafkaConsumer(
            *topics,
            loop=loop,
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            group_id=group_id,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
            fetch_max_wait_ms=fetch_max_wait_ms,
            fetch_max_bytes=fetch_max_bytes,
            fetch_min_bytes=fetch_min_bytes,
            max_partition_fetch_bytes=max_partition_fetch_bytes,
            request_timeout_ms=request_timeout_ms,
            retry_backoff_ms=retry_backoff_ms,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            auto_commit_interval_ms=auto_commit_interval_ms,
            check_crcs=check_crcs,
            metadata_max_age_ms=metadata_max_age_ms,
            partition_assignment_strategy=partition_assignment_strategy,
            max_poll_interval_ms=max_poll_interval_ms,
            rebalance_timeout_ms=rebalance_timeout_ms,
            session_timeout_ms=session_timeout_ms,
            heartbeat_interval_ms=heartbeat_interval_ms,
            consumer_timeout_ms=consumer_timeout_ms,
            max_poll_records=max_poll_records,
            ssl_context=ssl_context,
            security_protocol=security_protocol,
            api_version=api_version,
            exclude_internal_topics=exclude_internal_topics,
            connections_max_idle_ms=connections_max_idle_ms,
            isolation_level=isolation_level,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_password=sasl_plain_password,
            sasl_plain_username=sasl_plain_username,
            sasl_kerberos_service_name=sasl_kerberos_service_name,
            sasl_kerberos_domain_name=sasl_kerberos_domain_name,
            sasl_oauth_token_provider=sasl_oauth_token_provider)

    async def consumer_once_loop(self):
        await self.consumer.start()
        try:
            msg = await self.consumer.getone()
            while self.heartbeat.value:
                res = self.invoke_function(msg)

                tp = TopicPartition(msg.topic, msg.partition)

                res = await res

                if res:
                    await self.consumer.commit({tp: msg.offset + 1})
                    msg = await self.consumer.getone()
                else:
                    pass
                    #log

        finally:
            await self.consumer.stop()

    async def wrap_invoker(self, kv: Tuple[int, Any]):
        return (kv[0], await self.invoke_function(kv[1]))

    async def _async_batch_invoke_function(self, tp, batch):
        for k, v in await asyncio.gather(
                map(self.wrap_invoker, enumerate(batch))):
            if (not v) or isinstance(v, Exception):
                await self.consumer.commit({tp: batch[k].offset})
                return False
        await self.consumer.commit({tp: batch[k].offset + 1})
        return True

    async def async_consumer_batch_loop(self):
        assert asyncio.iscoroutine(
            self.invoke_function
        ), 'The invoke_function ({}) is not coroutine, while this function accepts only coroutine.'.format(
            type(self.invoke_function))

        await self.consumer.start()
        try:

            while self.heartbeat.value:
                msgs = await self.consumer.getmany()
                await asyncio.gather(
                    map(self._async_batch_invoke_function, msgs))

        finally:
            await self.consumer.stop()
