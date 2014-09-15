from TaskManager import TaskManager
from db import MongoDataAdapter
from time import sleep

# TODO Load config.
mongo = MongoDataAdapter.create_adapter('GatesPet')
max_tasks = 20
scan_interval = 5
process_interval = 120

# TaskManager instance.
tm = TaskManager(mongo, max_tasks=max_tasks, scan_interval=scan_interval)

# Fix tasks that previous instance did not proceed.
mongo.fix_tasks()

# Main loop
tm.start()

while True:
    tasks = mongo.get_scheduled_tasks()
    for task in tasks:
        tm.queue_task(task['module'], task['_id'], task['configure'], True)
        print "[pet] Task %s added." % task['_id']
        
    sleep(process_interval)
    
    
