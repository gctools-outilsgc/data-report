import sys
from subprocess import Popen, PIPE
import socket

class db_connection:
    
    def __init__(self):
        db_command = "kubectl port-forward -n message message-db-mongodb-replicaset-0 27017:27017 --context messageAKSprod-admin"
        self.port = 27017
    
        self.connect_to_database(db_command)

    def connect_to_database(self, command):
        self.connection = Popen(command, shell=True, stderr=sys.stderr, stdout=PIPE)
        
    def check_connection (self):
        s = socket.socket()        
        try:
            s.connect(("127.0.0.1", self.port))
            return 1
        except:
            return 0

    def terminate(self):
        self.connection.terminate()
        return 1

        
