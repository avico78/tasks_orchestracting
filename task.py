import pika


class TaskMsg:
  
  """
  
  Task actually instantiate inside Celery task - so not sure if should use asyncio ,
  
  Task push message to Manager exchange and update Manager with current status .
  Task consume message from it's own queue (queueu=task_uuid) - where  Manager can push message and notify task for next step(commit,rollback)
  based on what he recvied on Manager queue (it idetify by routing_key = task_uuid).

  In Celery it looks something like:
  
  
  @app.task(bind=True ,base=RuleBase,  name='proccess_rule')
  def proccess_rule(self, *args, **kwargs):

    main_id = kwargs.get('main_id')
    rule_uuid = kwargs.get('rule_uuid')
    source_type = kwargs.get('source_type')
    source_name = kwargs.get('source_name')
    ...
    ...
    from sqlalchemy.exc import SQLAlchemyError
    
      with TaskMsgBroker(connection_string=connection_string, queue_consumer=rule_uuid ,exchange_name='customer_1' ,routing_key=rule_uuid) as task_manager:
      
        if source_type == 'db':
          db_connection = self.db[source_name]
        load_operation.df_to_table(conn=db_connection, table_name=target_object_name ,df=df ,if_exists='append')
        try:
            db_connection.commit
            message['status'] = "OK"
            task_manager.produce(message=json.dumps(message))
            task_manager.consume(callback)          
        except SQLAlchemyError:
            message['status'] = "NOT_OK"
            task_manager.produce(message=json.dumps(message))
            task_manager.consume(callback)
  
  
  
  
  """

  
    def __init__(self, connection_string: str = None, queue_consumer: str = None ,exchange_name:str = None ,routing_key:str = None):
        self.connection_string = connection_string or f'amqp://guest:guest@localhost:5672/'
        self.queue_consumer = queue_consumer
        self.exchange_name = exchange_name
        self.routing_key = routing_key
    def __enter__(self):
        self.connection = pika.BlockingConnection(pika.URLParameters(self.connection_string))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_consumer)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.close()

    def consume(self, callback):
        self.channel.basic_consume(queue=self.queue_consumer, on_message_callback=callback, auto_ack=True)
        self.channel.start_consuming()

    def produce(self ,message):
        self.channel.basic_publish(exchange=self.exchange_name,
                        routing_key=self.routing_key,
                        body=message,
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                        )
                        )



def callback(ch, method, properties, body):
    data = body.decode()
    print("Manager decision:", data)
    if data == "commit":
        print("Got it ,commit the data")
    elif data == "rollback":
        print("Got it ,rollback preform")
    exit()


def run_task(i:int):
        
        connection_string = 'amqp://guest:guest@localhost:5672/'
        queue = 'customer_1'
        rules_uuid = list(map(str,range(1,number_of_tasks)))
        message = {'rule_id':1, 'main_id':1, 'rule_uuid':rules_uuid[i], 'status': None}

        with TaskMsg(connection_string=connection_string, queue_consumer=rules_uuid[i] ,exchange_name=queue ,routing_key=rules_uuid[i]) as task:
            try:
                5/(50-i)
                message['status'] = "OK"
                print(message)
                task.produce(message=json.dumps(message))
                task.consume(callback)
            except ZeroDivisionError:
                message['status'] = "NOT_OK"
                task.produce(message=json.dumps(message))
                task.consume(callback)



if __name__ == '__main__':

    import json
    import threading
    number_of_tasks=100
    
    threads = []
    
    for i in range(0,number_of_tasks):
        thread = threading.Thread(target=run_task, args=(i,))
        threads.append(thread)        

    for j in threads:
        j.start()


    for j in threads:
        j.join()

      
