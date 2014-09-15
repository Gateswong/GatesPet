from threading import Thread
from time import sleep
from Queue import Queue
import sys
import os

class TaskManager(Thread):
        
    def __init__(self, data_adapter, max_tasks=10, scan_interval=5):
        Thread.__init__(self)
        self.tasks = []
        self.queue = Queue()
        self.last_error = None
        self.running_tasks = 0
        self.config = {'max_tasks':max_tasks,
                       'scan_interval':scan_interval}
        self.data_adapter = data_adapter
    
    
    def exists_task(self, taskname):
        while True:
            try:
                return taskname in map(lambda x: x.config['name'], self.tasks + [item for item in self.queue.queue])
            except:
                pass
        return None
            
 
    def queue_task(self, module, taskname, config, force=False):
        sys.path.append(os.path.abspath(os.curdir) + '/module')
        m = __import__(module)
               
        if self.exists_task(taskname):
            if not force:
                self.last_error = 'Target task exists.'
                return False
            else:
                self.delete_task(taskname)
        
        task = m.create_task(taskname)
        task.push_config(config)
        self.queue.put(task)
        
        return True
    
    
    def delete_task(self, taskname):
        if taskname in map(lambda x: x.config['name'], self.tasks):
            task = filter(lambda x: x.config['name'] == taskname, self.tasks)[0]
            task.stop()
            self.tasks.remove(task)
        elif taskname in map(lambda x: x.config['name'], self.tasks):
            self.queue.queue.remove(filter(lambda x: x.config['name'] == taskname, self.queue.queue)[0])
        
        return None
    
    
    def push_config(self, dicts):
        """"""
        self.config.update(dicts)
    
    
    def run(self):
        while True:
            sleep(self.config['scan_interval'])
            for t in self.tasks:
                if not t.started:
                    self.data_adapter.push_task_result(t.config['name'],
                                                       {'last_error':t.result['last_error'],
                                                        'message':t.result['message'],
                                                        'startup_time':t.result['startup_time']},
                                                       t.result['schedule'])
                    self.tasks.remove(t)
            while(len(self.tasks) < self.config['max_tasks']):
                if self.queue.empty():
                    print '[TaskManager] No tasks in queue.'
                    break
                t = self.queue.get()
                self.tasks.append(t)
                t.start()
                print '[TaskManager] Task %s started!' % t.config['name']
                
    
    