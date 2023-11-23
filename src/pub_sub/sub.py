import asyncio
import json
import logging
import pickle
from typing import Callable, NoReturn, Union

import redis.asyncio as AsyncRedis
from redis.asyncio import RedisError
from redis.asyncio.client import PubSub

from .channels import (
    CHANNEL_PUBSUB_CLIENT_READY,
    CHANNEL_PUBSUB_TRACK,
    CHANNEL_PUBSUB_VIDEO_STREAM,
)

logger = logging.getLogger(__name__)


class MessageSubscribe:
    def __init__(
        self,
        host: str,
        port: Union[str, int],
        password: str,
        db: Union[str, int],
        sport_type: str,
    ) -> None:
        self.connect_info = dict(host=host, port=port, password=password, db=db)
        self._redis_connect()
        self.sport_type = sport_type

    @property
    def is_connected(self):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.rcon.ping())

    def _redis_connect(self):
        try:
            self.rcon = AsyncRedis.StrictRedis(
                **self.connect_info, health_check_interval=60
            )
            if self.is_connected:
                logger.info("Successfully connected to redis.")
        except (ConnectionError, Exception) as e:
            logger.error("Failed to connect to redis")
            raise e

    async def _redis_listen_with_retries(self, channel: str, ps: PubSub):
        retry_sleep = 1
        connect = False
        while True:
            try:
                if connect:
                    self._redis_connect()
                    await ps.subscribe(channel)
                    retry_sleep = 1
                async for message in ps.listen():
                    yield message
            except RedisError:
                logger.error(
                    "Cannot receive from redis... "
                    "retrying in "
                    "{} secs".format(retry_sleep)
                )
                connect = True
                await asyncio.sleep(retry_sleep)
                retry_sleep *= 2
                if retry_sleep > 60:
                    retry_sleep = 60

    async def _listen(self, _channel: str):
        async with self.rcon.pubsub() as ps:
            channel = _channel.encode("utf-8")
            await ps.subscribe(channel)
            async for message in self._redis_listen_with_retries(channel, ps):
                if message["type"] == "subscribe":
                    logger.debug(f"{message['channel'].decode(encoding='utf-8')} 订阅成功")
                elif (
                    message["channel"] == channel
                    and message["type"] == "message"
                    and "data" in message
                ):
                    yield message["data"]
            await ps.unsubscribe(channel)

    async def _thread(self, channel: str, callback: Callable[..., NoReturn]):
        while True:
            try:
                async for message in self._listen(channel):  # pragma: no branch
                    data = None
                    if isinstance(message, dict):
                        data = message
                    else:
                        if isinstance(message, bytes):  # pragma: no cover
                            try:
                                data = pickle.loads(message)
                            except:  # noqa: E722
                                pass
                        if data is None:
                            try:
                                data = json.loads(message)
                            except:  # noqa: E722
                                pass

                    logger.debug(f"{channel}: {data}")
                    callback(data)

            except asyncio.CancelledError:  # pragma: no cover
                break
            except:  # pragma: no cover  # noqa: E722
                import traceback

                logger.error(traceback.format_exc())

    async def get_student_info(self, callback: Callable[..., NoReturn]):
        """向终点服务发送绑定的学生信息和跑道信息以及起跑时间

        Args:
            payload (STUDENT_TRACK_START_RUN_INFOS): Students Infos: Include(student name, person_id, student_track_number, start_time)
        """
        _channel = f"{CHANNEL_PUBSUB_TRACK}:{self.sport_type}"
        await self._thread(channel=_channel, callback=callback)

    async def get_video_stream(self, callback: Callable[..., NoReturn]):
        """向终点服务发送视频是否录制的标识

        Args:
            payload (VIDEO_OP_START_STOP): {"op": "start"} or {"op": "stop"}
        """
        _channel = f"{CHANNEL_PUBSUB_VIDEO_STREAM}:{self.sport_type}"
        await self._thread(channel=_channel, callback=callback)

    async def get_client_ready(self, callback: Callable[..., NoReturn]):
        """起点向终点服务发送一些准备信息, 比如视频上传的路径

        Args:
            callback (Callable[..., NoReturn]): 回调函数, 接收准备信息
        """
        _channel = f"{CHANNEL_PUBSUB_CLIENT_READY}:{self.sport_type}"
        await self._thread(channel=_channel, callback=callback)
