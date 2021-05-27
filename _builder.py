#!/usr/bin/python3
__author__ = 'ziyan.yin'

import asyncio
import logging
from typing import List

import aiofiles
import os
import pickle

from ._publisher import Publisher

BUF_SIZE = 1024
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
    pass


class MessageQueue:

    def __init__(self, cwd='', maxsize=-1):
        self._cache = asyncio.Queue()
        self._cursor = 0
        self._maxsize = maxsize if maxsize > 0 else 1000
        self._cwd = cwd
        if os.path.exists(os.path.join(self._cwd, f'{repr(self)}_index')):
            try:
                with open(os.path.join(self._cwd, f'{repr(self)}_index'), 'r') as f:
                    self._cursor = int(f.readlines()[-1])
            except (ValueError, IndexError, TypeError):
                with open(os.path.join(self._cwd, f'{repr(self)}_index'), 'w') as f:
                    f.write('0')

    def __repr__(self):
        return 'message_queue'

    async def put(self, channel, data):
        await self.save(channel, data)
        await self._cache.put((channel, data))

    async def get(self):
        channel, data = await self._cache.get()
        return channel, data

    async def next(self):
        self._cursor += 1
        if self._cursor > self._maxsize:
            await self.reload()
        else:
            await self.save_cursor()

    def task_done(self):
        return self._cache.task_done()

    async def reload(self):
        del self._cache
        self._cache = asyncio.Queue()
        if os.path.exists(os.path.join(self._cwd, f'{repr(self)}_data')):
            os.rename(os.path.join(self._cwd, f'{repr(self)}_data'), os.path.join(self._cwd, f'{repr(self)}_data_tmp'))
        if os.path.exists(os.path.join(self._cwd, f'{repr(self)}_data_tmp')):
            async with aiofiles.open(os.path.join(self._cwd, f'{repr(self)}_data_tmp'), 'rb') as f:
                lines = await f.readlines()
                for index in range(len(lines)):
                    if index < self._cursor:
                        continue
                    channel, data = pickle.loads(lines[index])
                    await self.put(channel, data)
        async with aiofiles.open(os.path.join(self._cwd, f'{repr(self)}_index'), 'w') as f:
            await f.write('0\n')
            await f.flush()
        self._cursor = 0

    async def save(self, channel, data):
        async with aiofiles.open(os.path.join(self._cwd, f'{repr(self)}_data'), 'ab') as f:
            await f.write(memoryview(pickle.dumps((channel, data), protocol=5) + b'\n'))
            await f.flush()

    async def save_cursor(self):
        async with aiofiles.open(os.path.join(self._cwd, f'{repr(self)}_index'), 'a') as f:
            await f.write(str(self._cursor)+'\n')
            await f.flush()

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
    command = pickle.loads(message, encoding='utf-8')
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

    if consumers < 1:
        consumers = min(32, (os.cpu_count() or 1) + 4)
    for i in range(consumers):
        logger.info(f'Consumer {i + 1} started')
        consumer = MessageConsumer(worker.queue, publishers)
        asyncio.ensure_future(consumer.consume())

    async def handle(reader, writer):
        message = b''
        while True:
            pkg = await reader.read(BUF_SIZE)
            message += pkg
            if len(pkg) < BUF_SIZE:
                break
        try:
            channel, data = __create_task(message)
            await worker.produce(channel, data)
            res = data_format.copy()
            res['success'] = True
            writer.write(__struct(res))
        except ParamsError:
            res = data_format.copy()
            res['success'] = False
            res['message'] = '[10004]'
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
