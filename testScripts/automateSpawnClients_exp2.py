import subprocess
import sys
import time
from statistics import mean 

def performExperiments(numClients):
    codeDirectory = "/users/sanchit/consistency-at-the-edge/edgeServer"
    edgeServerLogins = {"server1":"root@c220g2-011331.wisc.cloudlab.us", 
        "server2":"root@c220g2-010631.wisc.cloudlab.us",
        "server3":"root@c220g2-010630.wisc.cloudlab.us", 
        "server4":"root@c220g2-011330.wisc.cloudlab.us"}
    modeList = ["erw"]
    #modeList = ["cr", "crw", "er", "erw"]
    file1 = open("lru2_results_exp2_part1.txt",'a')
    for mode in modeList:
        scriptToRun = None
        if (mode == "cr"):
            scriptToRun = "clientCentralServer_read.py"
        elif (mode == "crw"):
            scriptToRun = "clientCentralServer_readWrite_W1.py"
        elif (mode == "er"):
            scriptToRun = "clientEdgeServerTxRead.py"
        elif (mode == "erw"):
            scriptToRun = "clientEdgeServerFullCache.py"

        seeders = [1, 2, 3, 5, 7, 11, 13, 17, 
        19, 23, 29, 31, 37, 41, 43, 47, 
        53, 59, 61, 67, 71, 73, 79, 83, 
        89, 97, 101, 103, 107, 109, 113, 127, 
        131, 137, 139, 149, 151, 157, 163, 167, 
        173, 179, 181, 193, 197, 199, 211, 223, 
        233, 239, 241, 251, 257, 263, 269, 271, 
        277, 281, 283, 293, 307, 311, 313, 317]

        clientSize = []
        clientSize.append(int(numClients))

        print("mode: ", mode)
        print("************************")
        file1.write("mode: {}\n".format(mode))

        for nClients in clientSize:
            # start edgeServers
            '''
            for servername, edgeServerLogin in edgeServerLogins.items():
                subprocess.Popen(["ssh","-oStrictHostKeyChecking=no" ,"-i", 
                    "key", "%s"%(edgeServerLogin), "cd", 
                    "%s"%(codeDirectory) + ";",
                    "python3", "edgeServerLRU2.py", "%s"%(servername) + ";",
                    "exit"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE)
            # sleep so that client processes don't start before edgeServers do
            time.sleep(30)
            '''
            clients = []
            for i in range (nClients):    
                clients.append(subprocess.Popen(["python3", scriptToRun, 
                    str(i+1), str(numClients), str(seeders[i])], stdin=subprocess.PIPE, 
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
                    "key", "%s"%(edgeServerLogin), "kill", "-9", 
                    "$(pgrep python3);", "exit"], stdin=subprocess.PIPE, 
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # sleep
            time.sleep(30)            
            print("Average for",nClients,"clients: ", mean(clientAvg))
            file1.write("Average for {0} clients: {1}\n".format(nClients, 
                mean(clientAvg)))
        file1.write("\n\n==================================================\n\n")
    file1.close()

if __name__ == "__main__":
    if(len(sys.argv) != 2):
        print("usage: python3 spawnClient.py numClients")
        sys.exit(1)
    performExperiments(int(sys.argv[1]))
