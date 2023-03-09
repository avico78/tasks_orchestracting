import pika

class TaskMsgBroker:
  """
  
  Task actually instantiate inside Celery task - so not sure if can use asyncio ,
  Task consume message from it's queue (queueu=task_uuid) which on Manager can push message .,
  Task push message to Manager exchange which Manager consume from by only relavant task_uuid
  
  Task suppose to preform DB operation as insert data - and inform the Manager if it succuessed ,
  and then wait to Manager decide if it can commit the data or rollback.
  
  The decision based on all other tasks state .
  
  
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
        print("closing connection")
        self.connection.close()

    def consume(self, callback):
        # Task consume from it's on queue (which only manager can push)
        
        self.channel.basic_consume(queue=self.queue_consumer, on_message_callback=callback, auto_ack=True)
        self.channel.start_consuming()

    def produce(self ,message):
       # Task push to Manager exchange so Manager is aware on all Tasks state
        
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
        print("commit the data")
    elif data == "rollback":
        print("rollback preform")
    exit()


if __name__ == '__main__':
    import json

    connection_string = 'amqp://guest:guest@localhost:5672/'
    queue = 'customer_1'
    rule_uuid = 'c2ea6e9e-f7d8-4832-a807-662dcd09b8f9'


    ## this would be under celert task for inserting data for example


    message = {'rule_id':1, 'main_id':1, 'rule_uuid':rule_uuid, 'status': None}  

    with TaskMsgBroker(connection_string=connection_string, queue_consumer=rule_uuid ,exchange_name='customer_1' ,routing_key=rule_uuid) as task_manager:
        try:
            # just for illustratation
            #  enter integer for success or non int to failed 
            # the return state will push the message to Manager and manager will return commit/rollback 
            x = int(input("Enter a number: "))      
            message['status'] = "OK"
            print(message)
            task_manager.produce(message=json.dumps(message))
            task_manager.consume(callback)
        except ValueError:
            message['status'] = "NOT_OK"
            task_manager.produce(message=json.dumps(message))
            task_manager.consume(callback)

        
