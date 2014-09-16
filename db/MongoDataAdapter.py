import pymongo
from datetime import datetime

class MongoDataAdapter:
    
    def __init__(self, database='test'):
        self.mongo_client = pymongo.MongoClient()
        self.collection_error = self.mongo_client['errors']
        self.set_database(database)
    
    def set_database(self, db_name):
        self.db = self.mongo_client[db_name]
        self.collection_tasks = self.db['tasks']
        return None
        
        
    def fix_tasks(self):
        self.collection_tasks.update({'status':'queued'},
                                     {'$set':{'status':'waiting'}},
                                     multi=True)
        return None
    
    
    def update_task(self, name, module, schedule, configure={}, priority=0):
        """Insert or update a task."""
        task_id = self.collection_tasks.update({'_id':name}, 
                     {'module':module,
                      'schedule':schedule, 
                      'configure':configure,
                      'previous_result':{'last_error':None,
                                         'message':None,
                                         'startup_time':None},
                      'priority':priority,
                      'status':'waiting'}, True)
        return task_id
    
    
    def delete_task(self, name):
        """Delete a task."""
        self.collection_tasks.remove({'_id':name})
        return None
    
        
    def push_task_result(self, name, result, schedule): 
        task_id = self.collection_tasks.update({'_id':name},
                                          {'$set':{'previous_result':result,
                                                   'schedule':schedule}})
        return task_id
    
    
    def get_scheduled_tasks(self):
        # step 1: get scheduled tasks
        cursor = self.collection_tasks.find({'status':'waiting', 
                                             'schedule':{'$lt':datetime.utcnow()}}).sort('priority', pymongo.DESCENDING)
        tasks = list(cursor)
        
        # step 2: update status for these tasks
        for task in tasks:
            self.collection_tasks.update({'_id':task['_id']},
                                    {'$set':{'status':'queued'}})
        
        return tasks
    
    def push_error(self, module, error):
        self.collection_error.insert({'time':datetime.utcnow(), 'module':module, 'error':error})
        
        return None
    

def create_adapter(database='test'):
    adapter = MongoDataAdapter(database)
    return adapter