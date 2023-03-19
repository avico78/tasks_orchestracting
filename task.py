import pika
import time
from random import randint,choice

class TaskMsg:
    def __init__(self, connection_string: str = None, queue_task: str = None ,exchange_name:str = None ,routing_key:str = None):
        self.connection_string = connection_string or f'amqp://guest:guest@localhost:5672/'
        self.queue_task = queue_task
        self.exchange_name = exchange_name
        self.routing_key = routing_key
    def __enter__(self):
        self.connection = pika.BlockingConnection(pika.URLParameters(self.connection_string))
        self.channel = self.connection.channel()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        print("Exiting",self.routing_key)
       # self.channel.queue_delete(self.queue_task)   
        self.connection.close()

    def consume(self, callback):
        self.channel.basic_consume(queue=self.queue_task, on_message_callback=callback)
        self.channel.start_consuming()

    def produce(self ,message):
        self.channel.basic_publish(exchange=self.exchange_name,
                        routing_key=self.routing_key,
                        body=message,
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                        )
                        )



def callback(channel, method_frame, header_frame, body):
    # Once proof the approach , this function will be responsible for conn.rollback/commit

    data = body.decode()
    print(method_frame.delivery_tag)

    print("Manager decision:", data)
    if data == "commit":
        print("Got it ,commit the data")
    elif data == "rollback":
        print("Got it ,rollback preform")
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)        
    exit()


class TestFailed(Exception):
    
    def __init__(self,message:str):
        self.message = message
        super().__init__(message)
    def __str__(self):
        return f'Error: {self.message}'
    


def run_task(task_queue_id:str):

        connection_string = 'amqp://guest:guest@localhost:5672/'
        queue = 'customer_1'
        message = {'rule_id':1, 'main_id':1, 'rule_uuid':task_queue_id , 'status': None}
        print("creating task",message['rule_uuid'])
        with TaskMsg(connection_string=connection_string, queue_task=task_queue_id ,exchange_name=queue ,routing_key=task_queue_id) as task:

            try:            
               # some DB step done here (insert data )

               # failing on task 900 on purpose to get rollback
                if task_queue_id == "task_1_800":
                    print("Test - Failing Task #",i)
                    raise TestFailed(choice(["code_1","code_2"]))
                
                message['status'] = "OK"
                task.produce(message=json.dumps(message))
                task.consume(callback)
            except TestFailed as e:
                time.sleep(randint(1,5))
                message['status'] = e.message
                task.produce(message=json.dumps(message))
                task.consume(callback)



if __name__ == '__main__':

    import json
    import threading

    number_of_task = 100
    task_rule_ids = range(1,number_of_task)    
    routing_keys = [f'task_1_' + str(id)  for id in task_rule_ids]

    threads = []
    
    for routing_key in routing_keys:
        thread = threading.Thread(target=run_task, args=(routing_key,))
        threads.append(thread)        

    for j in threads:
        j.start()

    for j in threads:
        j.join()

      
