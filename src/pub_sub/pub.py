import asyncio
import logging
import pickle
from typing import Any, Union

from redis.asyncio import ConnectionError, RedisError, StrictRedis

from .channels import (
    CHANNEL_PUBSUB_CLIENT_READY,
    CHANNEL_PUBSUB_TRACK,
    CHANNEL_PUBSUB_VIDEO_STREAM,
)

logger = logging.getLogger(__name__)


class MessagePublish:
    def __init__(
        self, host: str, port: Union[str, int], password: str, db: Union[str, int]
    ) -> None:
        self.connect_info = dict(host=host, port=port, password=password, db=db)
        self._redis_connect()
        self.sport_type = None

    @property
    def sportType(self):
        return self.sport_type

    @sportType.setter
    def set_sport_type(self, type: str):
        self.sport_type = type

    @property
    def is_connected(self):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.rcon.ping())

    def _redis_connect(self):
        try:
            self.rcon = StrictRedis(**self.connect_info)

            if self.is_connected:
                logger.info("Successfully connected to redis.")

        except (ConnectionError, Exception) as e:
            logger.error("Failed to connect to redis")
            raise e

    async def _publish(self, channel: str, data: Any):
        retry = True
        try:
            if not retry:
                self._redis_connect()
            await self.rcon.publish(channel, pickle.dumps(data))
        except RedisError:
            if retry:
                logger.error("Cannot publish to redis... " "retrying")
                retry = False
            else:
                logger.error("Cannot publish to redis... " "giving up")

    async def publish_student_info(
        self,
        payload: Any,
    ):
        """向终点服务发送绑定的学生信息和跑道信息以及起跑时间

        Args:
            payload (Any): Students Infos: Include(student name, person_id, student_track_number, start_time)
        """
        try:
            if self.sportType:
                _channel = f"{CHANNEL_PUBSUB_TRACK}:{self.sportType}"
                logger.info(f"_channel: {_channel}")
                await self._publish(_channel, payload)
            else:
                raise ValueError(
                    f"Sport type is not specified. Missing: {self.sportType} "
                )
        except Exception as e:
            logger.error(e)

    async def publish_video_stream(self, payload: Any):
        """向终点服务发送视频是否录制的标识

        Args:
            payload (Any): {"op": "start"} or {"op": "stop"}
        """
        try:
            if self.sportType:
                _channel = f"{CHANNEL_PUBSUB_VIDEO_STREAM}:{self.sportType}"
                logger.info(f"_channel: {_channel}")
                await self._publish(_channel, payload)
            else:
                raise ValueError(
                    f"Sport type is not specified. Missing: {self.sportType} "
                )
        except Exception as e:
            logger.error(e)

    async def publish_client_ready(self, payload: Any):
        try:
            if self.sportType:
                _channel = f"{CHANNEL_PUBSUB_CLIENT_READY}:{self.sportType}"
                logger.info(f"_channel: {_channel}")
                await self._publish(_channel, payload)
            else:
                raise ValueError(
                    f"Sport type is not specified. Missing: {self.sportType} "
                )
        except Exception as e:
            logger.error(e)
