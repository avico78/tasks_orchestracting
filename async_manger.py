
from itertools import cycle
import aio_pika, sys, os,time
from aio_pika import ExchangeType
from aio_pika.abc import AbstractIncomingMessage
import json

import asyncio


class Manager:

    """
    Manager has dedciated exhcange and queue ,
    Where exhange bind only to relavant tasks  by task_uuid .

    Tasks will publish message with routing_key contain task_uuid for it state .
    So Manager can idetify what current Task status (error,success)

    Once all Tasks sent messages - Customer can decide for next step OR
    Base on "FAILED_ALL_ON_FIRST_ERROR" - it can immidetely notify all tasks to 
    rollback (endcase scenrio , Task should aborted if not started)
    
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
        self.routing_keys: ["task_rule_ids"] = list(map(str,routing_keys))
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
        else:
            print(f"The queue '{self.queue_name}' exists and empty")        

        # creating queue per task with auto_delete
        # seem safer having the task queue predefine before executing the task
        for task_queue in self.routing_keys:
             await self.channel.declare_queue(task_queue, auto_delete=True)
            

        await self._bind()
        

         
    async def consume(self):
        """
        Customer need to consume all tasks , and stop when all his tasks consumed .

        """
        print("Start consuming!")
        print(*self.routing_keys, sep = "\n")
        messages_to_consume = self.routing_keys.copy()
        async with self.queue.iterator() as queue_iter:           
            message: AbstractIncomingMessage
            async for message in queue_iter:
                async with message.process(ignore_processed=False):   
                        print("received messaged from task ", message.body.decode())
                        data = message.body.decode()
                        data_json = json.loads(data)
                        
                        if data_json["status"] in ["code_1","code_2"]:
                            print("Rollback required!!!")
                            await self.rollback_tasks()
                            return
                        messages_to_consume.remove(str(message.routing_key))

                        if len(messages_to_consume) == 0:
                            await self.commit_tasks()
                            break
          

    async def _bind(self):
        for routing_key in self.routing_keys:    
                await self.queue.bind(self.exchange, routing_key=routing_key)

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
            for routing_key in self.routing_keys:
                await self.publish(queue_name=routing_key, message_body="rollback",)
            
            return

    async def commit_tasks(self):
            for routing_key in self.routing_keys:
                await self.publish(queue_name=routing_key, message_body="commit")

            return


async def main():

    queue = 'customer_1'
    number_of_task = 100
    task_rule_ids = range(1,number_of_task)
    routing_keys = [f'task_1_' + str(id)  for id in task_rule_ids]
    consumer = Manager(queue_name=queue, exchange_name=queue, connection_string="amqp://guest:guest@localhost/",routing_keys=routing_keys)
    await consumer.run()



asyncio.run(main())


