from flask import Flask
from flask import jsonify, request,make_response
import time
import requests
import subprocess
import threading
 
app = Flask(__name__)

#shard_id_server_id.txt
@app.route("/primary_elect",methods = ["GET"])
def primary_election():
    print('primary election is going..', flush=True)
    try:        
        shard_id = request.args.get('shard_id')
        servers_list = request.args.get('server_ids').split(',')
        print('server_list is ', servers_list, flush=True)
        maxOperations = 0

        for server_id in servers_list:

            read_payload = {"server" : server_id, "shard": shard_id}
            try:
                response = requests.post(f'http://lbserver1:5000/helloread', json=read_payload).json()
                last_operation = response['message']
                print("last operation cnt is ", last_operation, flush=True)
            except Exception as e :
                print("error is ", e, flush=True)
            
            currentOperations = int(last_operation)
            if (currentOperations > maxOperations):
                maxOperations = currentOperations
                primary_server = server_id
 
            #fp.close()
        return jsonify({"message": "Primary server elected", "primary_server": primary_server, "status" : "success"}), 200
       
    except Exception as e:
        return jsonify({"error": f"An error occurred in primary election: {str(e)}"}), 500
   

# continuously check heartbeat
# Define the heartbeat function
def heartbeat():
    #global list_of_servers
    print('hearbeat checking...', flush=True)
    while True:
        response = requests.get('http://lbserver1:5000/getServerList').json()
        list_of_servers = response['servers']  # remove duplicates

        for server in list_of_servers:
            params = {"server" : server}
            
            try:
                response = requests.get(f'http://{server}:5000/heartbeat').json()
                if response.get('status') == 'success':
                    print(f"Server {server} is up and running.",flush=True)
                else:
                    time.sleep(10)
                    response = requests.get('http://lbserver1:5000/getServerList').json()
                    updated_list_of_servers = response['servers']  # remove duplicates
                    if server not in updated_list_of_servers:
                        continue

                    
                    list_of_servers.remove(server)
                    response = requests.delete('http://lbserver1:5000/rm', json= {'n':1, 'server':[server]})
                    print(f"Server {server} is down. Status code: {response.status_code}",flush=True)
                    response = requests.get('http://lbserver1:5000/getShardIdGivenServer', params=params).json()
                    shard_ids = response['shard_ids']
                    add_response = requests.post('http://lbserver1:5000/add', json={'n': 1,'new_shards':[],'servers':{f'{server}':shard_ids}})
                    print("response of heartbeat", add_response, flush=True)
                    if add_response['status'] == "success":
                        print("New server added successfully.")
                    else:
                        print("Failed to add a new server.")
            except requests.ConnectionError:

                time.sleep(10)
                response = requests.get('http://lbserver1:5000/getServerList').json()
                updated_list_of_servers = response['servers']  # remove duplicates
                if server not in updated_list_of_servers:
                    continue


                #print(f"Failed to connect to server {server}.",flush=True)
                response = requests.delete('http://lbserver1:5000/rm', json= {'n':1, 'servers':[server]}).json()
                print(response, flush=True)
                list_of_servers.remove(server)
                try:
                    response = requests.get('http://lbserver1:5000/getShardIdGivenServer', params=params).json()
                    shard_ids = response['shard_ids']
                    add_response = requests.post('http://lbserver1:5000/add', json={'n': 1,'new_shards':[],'servers':{f'{server}':shard_ids} }).json()
                    if add_response.get('status') == "success":
                        print("New server added successfully.")
                    else:
                        print("Failed to add a new server.")
                except Exception as e:
                    print('e is ', e, flush=True)
                    
               
        time.sleep(5)  # Wait for 5 seconds before checking again
 

if __name__ == "__main__":

    #Create a thread to run the heartbeat function
    time.sleep(50)
    heartbeat_thread = threading.Thread(target=heartbeat)
    heartbeat_thread.start()
    app.run(host = "0.0.0.0",debug = True)
    