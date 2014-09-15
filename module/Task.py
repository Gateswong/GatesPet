class Task:
    
    __type = 'Task'
    
    def __init__(self, taskname):
        self.config = {'name':taskname}
        self.result = {}
        
    def push_config(self, dicts):
        """"""
        self.config.update(dicts)

    def get_type(self):
        """"""
        return self.__type    

    def get_result(self, key):
        if self.result.has_key(key):
            return self.result[key]
        return None
    
    