import os
import sys
import logging
import signal
from argparse import ArgumentParser
from ConfigParser import SafeConfigParser
from pymongo import MongoClient
from imapclient import IMAPClient
from  threading import Thread
from Queue import Queue
from bson.binary import Binary


class LabelSync:
    def _getImapObject(self):
        config = self._config
        imap = IMAPClient(config.get('imap','host'),use_uid=True,ssl=config.getboolean('imap','ssl'))
        imap.debug = 0
        imap.login(config.get('imap','user'), config.get('imap','password'))
        imap.select_folder("[Gmail]/All Mail", True)
        return imap
        
    def _loadConfig(self,config_file):
        config = SafeConfigParser()
        config.read(config_file)
        self._config = config
    
    def _dbInit(self):
        config = self._config
        connection = MongoClient(config.get('database','host'),config.getint('database','port'))
        self._db = connection[config.get('database','database')]
        self._db.mail.ensure_index("raw.X-GM-MSGID")
    
    def _processField(self,key,value):
        if key == 'BODY.PEEK[]' or key == 'BODY[]':
            value = Binary(value)
        elif key == 'X-GM-THRID' or key == 'X-GM-MSGID':
            value = str(value)
        key = key.replace('.','_').replace('[]','')
        return (key,value)
        
    def _processMsg(self,msgs):
        for msg in msgs.iteritems():
            uid, contents = msg
            del contents['SEQ']
            contents = dict(self._processField(k, v) for k, v in contents.items())
            self._db.mail.update({'raw.X-GM-MSGID':contents['X-GM-MSGID']},{"$set": {'raw':contents}},upsert=True)
                
    def _imapWorker(self,threadNo):
        self._imap[threadNo] = self._getImapObject()
        logging.debug('Worker %d Start' % threadNo)
        imap = self._imap[threadNo]
        
        while True:
            try: 
                command = self._queue.get()
                msg_uid = command[0]
                if self._db.mail.find_one({'raw.X-GM-MSGID':str(command[1]['X-GM-MSGID'])}) != None:
                    logging.debug('Worker %d : Msg %d already done (%d left)' % (threadNo,command[1]['X-GM-MSGID'],self._queue.qsize()))
                    continue
                logging.debug('Worker %d : Msg %d (%d left)' % (threadNo,command[1]['X-GM-MSGID'],self._queue.qsize()))
                msgs = imap.fetch(msg_uid, ['X-GM-MSGID','X-GM-THRID','X-GM-LABELS','INTERNALDATE','FLAGS', 'RFC822.SIZE', 'BODY.PEEK[]', 'UID'])
                self._processMsg(msgs)
                self._queue.task_done()
            except BaseException, excep:
                logging.debug('Worker %d : Exception' % threadNo)
                logging.warning("something raised an exception: ",exc_info=excep)
                break
        logging.debug('Worker %d Logout' % threadNo)
        
    def _imapCleanup(self):
        logging.debug('Cleanup...')
        for i in range(self._num_workers):
            try:
                self._imap[i].logout()
            except:
                pass
        logging.debug('Done Cleanup...')

    def _mailfetch(self):
        logging.debug('IMAP Begin')
        imap = self._getImapObject()
        logging.debug('Fetching')
        msgs = imap.fetch('1:*', ['X-GM-MSGID'])
        imap.logout()
        logging.debug('Begin Thread')
        self._queue = Queue()
        
        self._num_workers = min(1,len(msgs))
        self._imap = {}
        signal.signal(signal.SIGINT, self._imapCleanup)
        for i in range(self._num_workers):
            t = Thread(target=self._imapWorker,args=(i,))
            t.daemon = True
            t.start()
        

        for msg in msgs.iteritems():
            self._queue.put(msg)
        
        self._queue.join()
        self._imapCleanup()
        
    def init(self):
        logging.basicConfig(level=logging.DEBUG)
        parser = ArgumentParser(description='Process some integers.')
        parser.add_argument('config_file', metavar='config', type=str, nargs='?', help='Configuration File', default = '~/.labelsyncrc')
        args = parser.parse_args()
        self._loadConfig(os.path.expanduser(args.config_file))
        self._dbInit()
        self._mailfetch()