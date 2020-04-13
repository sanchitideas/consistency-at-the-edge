import subprocess
import sys
import time
from statistics import mean 

def performExperiments():
    codeDirectory = "/users/aashish/consistency-at-the-edge/edgeServer"
    edgeServerLogins = {"server1":"root@c220g2-010629.wisc.cloudlab.us", 
        "server2":"root@c220g2-011303.wisc.cloudlab.us",
        "server3":"root@c220g2-010631.wisc.cloudlab.us", 
        "server4":"root@c220g2-010630.wisc.cloudlab.us"}
    modeList = ["er", "erw"]
    #modeList = ["cr", "crw", "er", "erw"]
    file1 = open("results_exp2.txt",'w')
    for mode in modeList:
        scriptToRun = None
        if (mode == "cr"):
            scriptToRun = "clientCentralServer_read.py"
        elif (mode == "crw"):
            scriptToRun = "clientCentralServer_readWrite.py"
        elif (mode == "er"):
            scriptToRun = "clientEdgeServer_read.py"
        elif (mode == "erw"):
            scriptToRun = "clientEdgeServer_readWrite.py"

        clientSize = [1, 2, 4, 8, 16, 32, 64]

        print("mode: ", mode)
        print("************************")
        file1.write("mode: {}\n".format(mode))

        for nClients in clientSize:
            # start edgeServers
            for servername, edgeServerLogin in edgeServerLogins.items():
                subprocess.Popen(["ssh","-oStrictHostKeyChecking=no" ,"-i", 
                    "key", "%s"%(edgeServerLogin), "cd", 
                    "%s"%(codeDirectory) + ";",
                    "python3", "edgeServer.py", "%s"%(servername) + ";",
                    "exit"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE)
            # sleep so that client processes don't start before edgeServers do
            time.sleep(30)
            clients = []
            for i in range (nClients):    
                clients.append(subprocess.Popen(["python3", scriptToRun, 
                    str(i+1)], stdin=subprocess.PIPE, 
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE))

            for i in range (nClients):
                clients[i].wait()
            outputs = []
            errors = []
            for i in range (nClients):
                o, e = clients[i].communicate()
                outputs.append(str(o, 'utf-8'))
                errors.append(str(e, 'utf-8'))
                #print(o)
                #print(e)

            clientAvg = []
            for i in range (nClients):
                o = outputs[i].split('\n')
                #print(o[1])
                #print(float(o[1].split(': ')[1]))
                clientAvg.append(float(o[1].split(': ')[1]))
            for edgeServerLogin in edgeServerLogins.values():
                subprocess.Popen(["ssh", "-oStrictHostKeyChecking=no", "-i", 
                    "key", "%s"%(edgeServerLogin), "cd", 
                    "%s"%(codeDirectory) + ";", "kill", "-9", 
                    "$(pgrep python3);", "exit"], stdin=subprocess.PIPE, 
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            print("Average for",nClients,"clients: ", mean(clientAvg))
            file1.write("Average for {0} clients: {1}\n".format(nClients, 
                mean(clientAvg)))
        file1.write("\n\n==================================================\n\n")
    file1.close()

if __name__ == "__main__":
    if(len(sys.argv) != 1):
        print("usage: python3 spawnClient.py")
        sys.exit(1)
    performExperiments()