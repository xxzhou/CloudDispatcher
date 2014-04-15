import logging
logger = logging.getLogger("convirt.services.model")

class AtomRequest:
    """
    """
    def __init__(self,auth,image_id,config,mode,node_id=None,priority=None,group_id=None,dom_id=None,vm_name=None,date=None,time=None,subuser=False,_dc=None,task_id=None):
        logger.debug('AtomRequest initialized') 
        self.image_id = image_id
        self.config = config
        self.mode = mode
        self.node_id = node_id
        self.priority = priority
        self.group_id = group_id
        self.dom_id = dom_id
        self.vm_name = vm_name
        self.date = date
        self.time = time
        self.subuser = subuser
        self._dc = _dc
        self.auth = auth
        self.task_id = task_id
        
