import sys
from subprocess import Popen, PIPE, check_output
import socket
import pandas as pd

class db_connection:
    
    def __init__(self):
        db_command = "/usr/local/bin/kubectl port-forward --context CollabAKSProd-admin -n wiki " + self.__get_name() + " 3306:3306"
        self.port = 3306
        
        self.connect_to_database(db_command)

    #Not always the same name, so this method fetches the db name
    def __get_name(self):
        cmd = check_output("/usr/local/bin/kubectl get pods --context CollabAKSProd-admin -n wiki", shell=True) #maintains synch order
        list_of_db = cmd.decode().split("\n"[0])

        def startswith(var):
            return var.startswith("wiki-db-deployment")

        filtered_list = filter(startswith, list_of_db)

        for db in filtered_list: 
            return db.split()[0]
            
    #Forms connection
    def connect_to_database(self, command):
        self.connection = Popen(command, shell=True, stderr=sys.stderr, stdout=PIPE)
        self.socket = socket.socket()

    #used to check if able to connect before querying
    def check_connection (self):       
        try:
            self.socket.connect(("127.0.0.1", self.port))
            return 1
        except:
            return 0

    def terminate(self):
        self.socket.close()
        self.connection.terminate()

        
