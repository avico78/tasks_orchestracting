# tasks_orchestracting
review conecpt




Manager :

This part of main program which basically
runs mutliple Managers(customers) each runs it's own tasks- tasks submited to celery workers .

The program running asyncio and that reason manager using iot-pika supporting async 



Brooker:
1.Consumer:
Consume messages from tasks assiosiate with current run - in real world,
Multiple Manager running at same time ,each Manager with it's own tasks.

2.Publish:
Manager publish to all tasks queue message for either abort or rollbac


Task:

Tasks are running in celery ,
Tasks preform insert to DB in exampale , to preform commit - all tasks under same Manager must be successfully otherwise failed.
To support it , each celery task also "comminicate" with Manager by the following:

1.Consumer/listen to it's own queue and waits for insturctions for commit/rollback.
2.Push message to Manager exchange and update with commit attempt resolt (success/failed).
If single task failed - manager send rollback message to all tasks.




