
from itertools import cycle
import aio_pika, sys, os,time
from aio_pika import ExchangeType
from aio_pika.abc import AbstractIncomingMessage
import json

import asyncio


class Manager:

    """
    Manager has dedciated exhcange and queue "customer_<#>" ,
    Where exhange bind with tasks by the task_uuid .
    
    And each Task can publish to this queue with currernt state with it's uuid.
    So Manager can idetify what current Task status (error,success).
        
    if any tasks publish "NOT_OK" to Manager exchange -> Manager send rollback to all tasks at once,
    
    
    """


    def __init__(self, exchange_name, queue_name: str, connection_string: str = None, routing_keys: list = None):
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.connection_string = connection_string or f'amqp://guest:guest@localhost:5672/'
        self.connection = None
        self.exchange = None
        self.channel = None
        self.queue = None
        self.routing_keys: ["tasks_uuid"] = list(map(str,routing_keys))
        self.tasks_state = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.connection_string)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=10)
        
        self.exchange = await self.channel.declare_exchange(
           self.exchange_name, ExchangeType.DIRECT,auto_delete=False
        )

        self.queue = await self.channel.declare_queue(self.queue_name ,exclusive=True)
        if self.queue.declaration_result.message_count > 0:
            print(f"The queue '{self.queue_name}' exists and has {self.queue.declaration_result.message_count} messages")
            await self.queue.purge()
            print("After delete", self.queue.declaration_result.message_count)
        else:
            print(f"The queue '{self.queue_name}' exists and empty")        


        await self._bind()

         
    async def consume(self):
        """
        Customer need to consume all tasks , and stop when all his tasks consumed .

        """
        print("Start consuming!")
        messages_to_consume = self.routing_keys.copy()

        async with self.queue.iterator() as queue_iter:
            message: AbstractIncomingMessage
            async for message in queue_iter:
                async with message.process():   
                       if str(message.routing_key) in messages_to_consume:
                            print(message.body.decode())
                            data = message.body.decode()
                            data_json = json.loads(data)
                   
                            if data_json["status"] == "NOT_OK":
                                await self.rollback_tasks()
                            messages_to_consume.remove(str(message.routing_key))

                            if len(messages_to_consume) == 0:
                                await self.commit_tasks()
                                break


    async def _bind(self):
        # binding all routing kets to Manager exhange
        for task_uuid in self.routing_keys:    
                await self.queue.bind(self.exchange, routing_key=task_uuid)

    async def close(self):
        if self.connection is not None:
            await self.connection.close()

    async def run(self):
        await self.connect()
        await self.consume()
        await self.close()


    async def publish(self,queue_name,message_body):
        print(f'publishing:{message_body} for {queue_name}')
        queue = await self.channel.declare_queue(queue_name)
        message = aio_pika.Message(body=message_body.encode("utf-8"))
        await self.channel.default_exchange.publish(
                aio_pika.Message(body=message),
                routing_key=queue_name
            )

    async def rollback_tasks(self):
        
            for task_uuid in self.routing_keys:
                await self.publish(queue_name=str(task_uuid),message_body="rollback")
            
            exit()

    async def commit_tasks(self):
            for task_uuid in self.routing_keys:
                await self.publish(queue_name=str(task_uuid),message_body="commit")

            exit()

async def main():
# for demo , will inititate the Manager by 100 tasks 
# Which mean the Manager exchange associate by 100 routing_keys (in real world each tasks has it's own uuid)

    number_of_tasks=100
    routing_keys = list(map(str,range(1,number_of_tasks)))
    consumer = Manager(queue_name="customer_1",exchange_name="customer_1", connection_string="amqp://guest:guest@localhost/",routing_keys=routing_keys)
    await consumer.run()


asyncio.run(main())





