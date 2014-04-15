import logging
logger = logging.getLogger("convirt.services.model")
from Queue import Queue
class MessageQueue:
    """
        MessageQueue, FIFO 
    """
    def __init__(self):
        logger.debug('MessageQueue initialized')
        self.queue = Queue() 
    def putMessage(self,message):
        self.queue.put(message)
    def getMessage(self):
        return self.queue.get()
    def messageDone(self):
	self.queue.task_done()
    def getSize(self):
        return self.queue.qsize()
