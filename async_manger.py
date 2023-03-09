
from itertools import cycle
import aio_pika, sys, os,time
from aio_pika import ExchangeType
from aio_pika.abc import AbstractIncomingMessage
import json

import asyncio


class Manager:

    """
    Manager based on async as whole program based asyncio .
    
    
    Manager has dedciated exhcange and queue to consume from,
    Where exhange bind only to relavant tasks by task_uuid .
    So Manager consume /listen to all tasks messages 
    + Manager publish message to specific task (queue=task_uuid)
    
    
    Task will publish message with routing_key contain task_uuid for it state (to Manager).
    So Manager can idetify what current Task status (error,success)
    + Task consume for it's own queue (to get insturction from Manager)
    
    Manager decide to  commit/rollback to all tasks in case single task return "OK"/"NOT_OK" messages .
    
    
    """
    # FAILED_ALL_ON_FIRST_ERROR:bool = True

    def __init__(self, exchange_name, queue_name: str, connection_string: str = None, routing_keys: list = None):
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.connection_string = connection_string or f'amqp://guest:guest@localhost:5672/'
        self.connection = None
        self.exchange = None
        self.channel = None
        self.queue = None
        self.routing_keys: ["tasks_uuid"] = list(map(str,routing_keys)) # required as in program using real uuid.hex
        self.tasks_state = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.connection_string)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=10)
        
        self.exchange = await self.channel.declare_exchange(self.exchange_name, ExchangeType.DIRECT,
                                                            auto_delete=False
                                                           )

        self.queue = await self.channel.declare_queue(self.queue_name ,exclusive=True)
        
        if self.queue.declaration_result.message_count > 0:
            print(f"The queue '{self.queue_name}' exists and has {self.queue.declaration_result.message_count} messages")
            await self.queue.purge()
            print("After delete", self.queue.declaration_result.message_count)
        else:
            print(f"The queue '{self.queue_name}' exists and empty")        

      # binding routing_key with exchange
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
                            data = message.body.decode()
                            data_json = json.loads(data)
                   
                            if data_json["status"] == "NOT_OK":
                                await self.rollback_tasks()
                            messages_to_consume.remove(str(message.routing_key))

                            if len(messages_to_consume) == 0:
                                await self.commit_tasks()
                                break


    async def _bind(self):
        for task_uuid in self.routing_keys:    
                await self.queue.bind(self.exchange, routing_key=task_uuid)

    async def close(self):
        print("Closing connection")
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
            """
            TODO:
            Add support for updating the tasks only after all tasks messages 
            recivied(all tasks on final stage-commit/rollback)
            """
            for task_uuid in self.routing_keys:
                await self.publish(queue_name=str(task_uuid),message_body="rollback")
        
            exit()

    async def commit_tasks(self):
            for task_uuid in self.routing_keys:
                await self.publish(queue_name=str(task_uuid),message_body="commit")

            exit()

async def main():
  
    routing_keys = ['c2ea6e9e-f7d8-4832-a807-662dcd09b8f9']
    consumer = Manager(queue_name="customer_1",exchange_name="customer_1", connection_string="amqp://guest:guest@localhost/",routing_keys=routing_keys)
    await consumer.run()




asyncio.run(main())



