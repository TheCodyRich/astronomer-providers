import asyncio
from typing import Any, AsyncGenerator, Dict, Optional

from astronomer.providers.amazon.aws.hooks.base_aws_async import AwsBaseHookAsync


class AwsLogsHookAsync(AwsBaseHookAsync):
    """
    Interact with AWS CloudWatch Logs using aiobotocore python library

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["client_type"] = "logs"
        super().__init__(*args, **kwargs)

    async def describe_log_streams_async(
        self, log_group: str, stream_prefix: str, order_by: str, count: int
    ) -> Dict[str, Any]:
        """
        Async function to get the Lists the log streams for the specified log group.
        You can list all the log streams or filter the results by prefix. You can also control
        how the results are ordered.

        :param log_group: The name of the log group.
        :param stream_prefix: The prefix to match.
        :param order_by: f the value is LogStreamName , the results are ordered by log stream name.
         If the value is LastEventTime , the results are ordered by the event time. The default value is LogStreamName.
        :param count: The maximum number of items returned
        """
        async with await self.get_client_async() as client:
            try:
                response: Dict[str, Any] = await client.describe_log_streams(
                    logGroupName=log_group,
                    logStreamNamePrefix=stream_prefix,
                    orderBy=order_by,
                    limit=count,
                )
                return response
            except client.exceptions.ResourceNotFoundException:
                # On the very first training job run on an account, there's no log group until
                # the container starts logging, so ignore any errors thrown about that
                pass

    async def get_log_events(
        self,
        log_group: str,
        log_stream_name: str,
        start_time: int = 0,
        skip: int = 0,
        start_from_head: bool = True,
    ) -> AsyncGenerator:
        """
        A generator for log items in a single stream. This will yield all the
        items that are available at the current moment.

        :param log_group: The name of the log group.
        :param log_stream_name: The name of the specific stream.
        :param start_time: The time stamp value to start reading the logs from (default: 0).
        :param skip: The number of log entries to skip at the start (default: 0).
            This is for when there are multiple entries at the same timestamp.
        :param start_from_head: whether to start from the beginning (True) of the log or
            at the end of the log (False).
        :rtype: dict
        :return: | A CloudWatch log event with the following key-value pairs:
                 |   'timestamp' (int): The time in milliseconds of the event.
                 |   'message' (str): The log event data.
                 |   'ingestionTime' (int): The time in milliseconds the event was ingested.
        """
        next_token = None
        while True:
            if next_token is not None:
                token_arg: Optional[dict[str, str]] = {"nextToken": next_token}
            else:
                token_arg = {}

            async with await self.get_client_async() as client:
                response = await client.get_log_events(
                    logGroupName=log_group,
                    logStreamName=log_stream_name,
                    startTime=start_time,
                    startFromHead=start_from_head,
                    **token_arg,
                )

                events = response["events"]
                event_count = len(events)

                if event_count > skip:
                    events = events[skip:]
                    skip = 0
                else:
                    skip -= event_count
                    events = []

                for event in events:
                    await asyncio.sleep(1)
                    yield event

                if next_token != response["nextForwardToken"]:
                    next_token = response["nextForwardToken"]
                else:
                    return