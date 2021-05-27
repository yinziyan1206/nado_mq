#!/usr/bin/python3
__author__ = 'ziyan.yin'


class Publisher:
    _subscribe = set()

    async def publish(self, data):
        raise NotImplementedError()

    @classmethod
    def subscribe(cls, channel):
        cls._subscribe.add(channel)

    @classmethod
    def contains_channel(cls, channel):
        return channel in cls._subscribe
