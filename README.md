# tasks_orchestracting
***learning with mr.Radek***


What do we have here :

Manager script - will wait to 100 message coming from tasks , if any message through an error ,
Manager will send "rollback" message to Task ,
All tasks will preform rollback 


How to run:

Rabbitmq avilable - can run docker compose 
Open 2 sesssions and execute 

first -> Manager scipt , it will wait for tasks messagess
Second execute task.py - a 100 "like" tasks will be running, 
task #50 will fail

due to :
```
...
       try:
                5/(50-i)
                message['status'] = "OK"
                print(message)
                task.produce(message=json.dumps(message))
                task.consume(callback)
```

Result:

Manager will send rollback to tasks.
All task will preform rollback .


If you want to see "success" - where all message preform commit ,
change  5/(50-i) to something will not through ZeroDivisionError.



![image](https://user-images.githubusercontent.com/9049952/224170605-b7fdde59-f8f1-41b6-a3e3-5b7ac8f48503.png)
               



HL explanation :

The idea is to provide celery tasks a conditinal commit .
conditinal commit means :

if there's 100 celery tasks preforming data injections to different database ,
if one of them failed - preform rollback for all.
and only if tasks passed inject the data with no error - all task notify to commit.

to solve it the idea is to have Manager which do real-time monitroing on all tasks -
if any one failed , all tasks will notify to preform rollback/undo .


This class used part of main program where mutliple Managers(customers) exuecuted ,
each runs it's own set of tasks -> (tasks submited to celery workers).
The program running asyncio and for that reason this write in async . 



Manager - Brooker side:

1.Manager Consume messages from manager excahnage which associate with all tasks by task uuid ,
(in real world multiple Managers running at same time ,each Manager with it's own tasks)

2.Manager Publish:
Manager publish to all tasks dedciated queues message for either commit or rollbac


Task:

Tasks are running in celery ,
Tasks preform insert to DB in exampale , to preform commit - all tasks under same Manager must be successfully otherwise failed.
To support it , each celery task also "comminicate" with Manager by the following:

1.Consumer/listen to it's own queue and waits for insturctions for commit/rollback.
2.Push message to Manager exchange and update with commit attempt resolt (success/failed).
If single task failed - manager send rollback message to all tasks.




