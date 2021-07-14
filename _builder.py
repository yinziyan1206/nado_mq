#!/usr/bin/python3
__author__ = 'ziyan.yin'

import asyncio
import logging
from typing import List

import os
import pickle

from ._publisher import Publisher

BUF_SIZE = 1024
MAX_SIZE = 2**20 * 5
HOST = ''
PORT = 11210

data_format = {
    'success': True,
    'data': '',
    'message': '',
    'code': 0,
}
logger = logging.getLogger('MQ')


class ParamsError(Exception):

    def __str__(self):
        return '[10004]'


class OutOfBoundError(Exception):

    def __str__(self):
        return '[10005]out of bounds'


class MessageQueue:

    def __init__(self, cwd='', maxsize=-1):
        self._cursor = 0
        self._maxsize = maxsize if maxsize > 0 else 0
        self._cache = asyncio.Queue(maxsize=self._maxsize)
        self._cwd = cwd

    def __repr__(self):
        return 'message_queue'

    async def put(self, channel, data):
        await self._cache.put((channel, data))

    async def get(self):
        channel, data = await self._cache.get()
        return channel, data

    async def next(self):
        self._cursor += 1

    def task_done(self):
        return self._cache.task_done()

    async def reload(self):
        self._cursor = 0

    def empty(self):
        return self._cache.empty()

    def qsize(self):
        return self._cache.qsize()


class MessageWorker:

    __slots__ = ['queue']

    def __init__(self, cwd='', maxsize=-1):
        self.queue = MessageQueue(cwd=cwd, maxsize=maxsize)

    async def initial(self):
        await self.queue.reload()

    async def produce(self, channel, data):
        await self.queue.put(channel, data)
        logger.info(f'{channel} input')


class MessageConsumer:

    __slots__ = ['publishers', 'queue']

    def __init__(self, queue: MessageQueue, publishers: List[Publisher]):
        self.queue = queue
        self.publishers = publishers

    def register(self, publisher: Publisher):
        self.publishers.append(publisher)

    async def consume(self):
        while True:
            channel, data = await self.queue.get()
            try:
                for publisher in self.publishers:
                    if publisher.contains_channel(channel):
                        await publisher.publish(data)
                        logger.info(f'{channel} publish to {publisher.__class__.__name__}')
            except Exception as ex:
                logger.error(f'{channel}:[{ex.__class__.__name__}] {ex}')
            finally:
                await self.queue.next()


def __create_task(message):
    command = pickle.loads(message)
    if 'channel' in command and 'data' in command:
        return command['channel'], command['data']
    else:
        raise ParamsError()


def __struct(data):
    data = pickle.dumps(data, protocol=5)
    length = '00000000000{0}'.format(len(data))
    return memoryview(('%s' % (length[-12:])).encode() + data)


def setup(
    work_dir: str = '',
    port: int = PORT,
    publishers: List[Publisher] = None,
    maxsize: int = -1,
    consumers: int = -1
):
    loop = asyncio.get_event_loop()

    worker = MessageWorker(work_dir, maxsize)
    loop.run_until_complete(worker.queue.reload())

    consumers = consumers if consumers > 0 else min(32, (os.cpu_count() or 1) + 4)
    for i in range(consumers):
        logger.info(f'Consumer {i + 1} started')
        consumer = MessageConsumer(worker.queue, publishers)
        asyncio.ensure_future(consumer.consume())

    async def handle(reader, writer):
        message = b''
        total = 0
        try:
            while True:
                pkg = await reader.read(BUF_SIZE)
                message += pkg
                if len(pkg) < BUF_SIZE:
                    break
                total += BUF_SIZE
                if total > MAX_SIZE:
                    raise OutOfBoundError()

            channel, data = __create_task(message)
            await worker.produce(channel, data)
            res = data_format.copy()
            res['success'] = True
            writer.write(__struct(res))
        except (ParamsError, OutOfBoundError) as ex:
            res = data_format.copy()
            res['success'] = False
            res['message'] = str(ex)
            writer.write(__struct(res))
        finally:
            await writer.drain()
            writer.close()

    coro = asyncio.start_server(handle, '', port)
    server = loop.run_until_complete(coro)

    # Serve requests until Ctrl+C is pressed
    logger.info('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        # Close the server
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()
        raise
