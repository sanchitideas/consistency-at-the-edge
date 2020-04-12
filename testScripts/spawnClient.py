import subprocess
import sys
import time
from statistics import mean 

if(len(sys.argv) != 3):
    print("usage: python3 spawnClient.py <mode(cr/crw/er/erw)> <numOfClients>")
    sys.exit(1)

mode = sys.argv[1]
nClients = int(sys.argv[2])
scriptToRun = None
if (mode == "cr"):
    scriptToRun = "clientCentralServer_read.py"
elif (mode == "crw"):
    scriptToRun = "clientCentralServer_readWrite.py"
elif (mode == "er"):
    scriptToRun = "clientEdgeServer_read.py"
elif (mode == "erw"):
    scriptToRun = "clientEdgeServer_readWrite.py"

clients = []
for i in range (nClients):    
    clients.append(subprocess.Popen(["python3", scriptToRun, str(i+1)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE))

for i in range (nClients):
    clients[i].wait()


outputs = []
errors = []
for i in range (nClients):
    o, e = clients[i].communicate()
    outputs.append(str(o, 'utf-8'))
    errors.append(str(e, 'utf-8'))

clientAvg = []
for i in range (nClients):
    o = outputs[i].split('\n')
    #print(o[1])
    #print(float(o[1].split(': ')[1]))
    clientAvg.append(float(o[1].split(': ')[1]))

print("Average for",nClients,"clients: ", mean(clientAvg))
