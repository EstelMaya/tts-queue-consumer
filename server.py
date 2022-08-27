import asyncio
from functools import partial
from aio_pika import connect_robust, Message
from os import getuid, getenv
from aiohttp.web import Response, Application, AppRunner, TCPSite
from aiohttp import ClientSession


RABBIT_HOST = getenv('RABBIT_HOST', 'rabbitmq.example.com')
RABBIT_USER = getenv('RABBIT_USER', 'admin')
RABBIT_PASS = getenv('RABBIT_PASS', '')
RABBIT_PORT = int(getenv('RABBIT_PORT', 5672))
QUEUE = 'tts_queue'

NRT_TTS_URL = getenv('TTS_URL_NRT', '')
RT_TTS_URL = getenv('TTS_URL_RT', '')
METRIC_URL = getenv('METRIC_URL', '')

# Maximal amount of parallel connections to TTS server
RT_CONNECTIONS_LIM = 3  # Realtime
NRT_CONNECTIONS_LIM = 25  # Non-realtime

# Time of entry expiration in RabbitMQ
RABBIT_EXPIRATION_SEC = 600


class DynamicSemaphore:
    def __init__(self, value=1):
        self.cap = value
        self.free = value
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        async with self._lock:
            while self.free <= 0:
                await asyncio.sleep(0.1)
        self.free -= 1

    async def __aexit__(self, *_):
        self.free += 1

    def update(self, new_value):
        diff = new_value-self.cap
        self.cap = new_value
        self.free += diff


rt_semaphore = DynamicSemaphore(RT_CONNECTIONS_LIM)
nrt_semaphore = DynamicSemaphore(NRT_CONNECTIONS_LIM)


async def reply(data, message, exchange):
    await exchange.publish(Message(data,
                                   correlation_id=message.correlation_id,
                                   expiration=RABBIT_EXPIRATION_SEC),
                           routing_key=message.reply_to)


async def on_message(exchange, message):
    async with nrt_semaphore:
        async with message.process():
            msg = message.body.decode()

            print('sending data to TTS...')
            async with ClientSession() as session:
                async with session.post(NRT_TTS_URL, data=msg) as r:
                    response = await r.content.read()

            await reply(response, message, exchange)
    print("Request complete")


async def on_realtime(exchange, message):
    async with rt_semaphore:
        async with message.process():
            msg = message.body.decode()

            print('sending realtime...')
            async with ClientSession() as session:
                async with session.post(RT_TTS_URL, data=msg) as r:
                    while (chunk := await r.content.read(2**14)):
                        await reply(chunk, message, exchange)
                    await reply(0, message, exchange)
    print("Request complete")


async def update_semaphore():
    previous_pods = 0
    while True:
        async with ClientSession() as session:
            async with session.get(METRIC_URL, ssl=False) as r:
                msg = await r.json()
                n_pods = len(msg.get('items', []))
                if n_pods != previous_pods:
                    print(f'{n_pods} pods is now available, changing workload...')
                    nrt_semaphore.update(NRT_CONNECTIONS_LIM*n_pods)
                    previous_pods = n_pods
        await asyncio.sleep(15)


async def consumer():
    connection = await connect_robust(login=RABBIT_USER, password=RABBIT_PASS, host=RABBIT_HOST, port=RABBIT_PORT)

    channel = await connection.channel()

    queue = await channel.declare_queue(QUEUE, durable=True)
    queue_realtime = await channel.declare_queue('tts_realtime', durable=True)

    await queue.consume(partial(on_message, channel.default_exchange))
    await queue_realtime.consume(partial(on_realtime, channel.default_exchange))


async def healthcheck():
    app = Application()
    app.router.add_get(
        '/', lambda _: Response(content_type='application/json'))
    runner = AppRunner(app)
    await runner.setup()
    await TCPSite(runner, '0.0.0.0', 8080 if getuid() else 80).start()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(update_semaphore())
    loop.create_task(healthcheck())
    loop.create_task(consumer())

    print("Waiting for messages. To exit press CTRL+C")
    loop.run_forever()
