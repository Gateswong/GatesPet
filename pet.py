from TaskManager import TaskManager
from db import MongoDataAdapter
from time import sleep
from daemon.pidfile import PIDLockFile
import psutil

def create_task_manager(db, max_tasks, scan_interval):
    # TaskManager instance.
    tm = TaskManager(mongo, max_tasks=max_tasks, scan_interval=scan_interval)
    return tm

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
max_tasks = 50
scan_interval = 10
process_interval = 120

tm = create_task_manager(mongo, max_tasks, scan_interval)

# Fix tasks that previous instance did not proceed.
mongo.fix_tasks()

# Main loop
tm.start()

print 'Main program started.'
while True:
    try:
        if not tm.isAlive():
            tm = create_task_manager(mongo, max_tasks, scan_interval)
            tm.start()
            
        tasks = mongo.get_scheduled_tasks()
        for task in tasks:
            tm.queue_task(task['module'], task['_id'], task['configure'], True)
            # print "[pet] Task %s added." % task['_id']
    except StandardError as err:
        mongo.push_error('pet', {'message':err.args, 'position':'pet.py >> main loop'})
        
    sleep(process_interval)
        
