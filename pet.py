from TaskManager import TaskManager
from db import MongoDataAdapter
from time import sleep
from daemon.pidfile import PIDLockFile
import psutil

lock = PIDLockFile('pet.pid')

if lock.is_locked() and not lock.i_am_locking():
    if not psutil.pid_exists(lock.read_pid()):
        print 'The pidlock is hold by a void process. Breaking it.'
        lock.break_lock()
    else:
        print 'Another process is holding pidlock. Exit.'
        exit()

print 'Acquiring pidlock.'
lock.acquire(timeout=5)

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

print 'Main program started.'
while True:
    try:
        tasks = mongo.get_scheduled_tasks()
        for task in tasks:
            tm.queue_task(task['module'], task['_id'], task['configure'], True)
            # print "[pet] Task %s added." % task['_id']
    except BaseException as err:
        mongo.push_error('pet', {'message':err.args, 'position':'pet.py >> main loop'})
        
    sleep(process_interval)
        
