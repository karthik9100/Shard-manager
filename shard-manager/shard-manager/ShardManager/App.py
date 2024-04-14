from flask import Flask
from flask import jsonify, request,make_response
import time
import requests
from LoadBalancer.App import list_of_servers
from LoadBalancer.App import get_shardid_given_server
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
# def heartbeat():
#     global list_of_servers
#     print('hearbeat checking...', flush=True)
#     while True:
#         list_of_servers = list(set(list_of_servers))   # remove duplicates

#         for server in list_of_servers:
#             try:
#                 response = requests.get(f'http://{server}:5000/heartbeat')
#                 if response.status_code == 200:
#                     print(f"Server {server} is up and running.",flush=True)
#                 else:
#                     shard_ids = get_shardid_given_server(None,server)
#                     print('before remove of servers', list_of_servers, flush=True)
#                     list_of_servers.remove(server)
#                     print(f"Server {server} is down. Status code: {response.status_code}",flush=True)
#                     add_response = requests.post('http://127.0.0.1:5000/add', json={'n': 1,'new_shards':[],'servers':{f'{server}':shard_ids}})
#                     if add_response.status_code == 200:
#                         print("New server added successfully.")
#                     else:
#                         print("Failed to add a new server.")
#             except requests.ConnectionError:
#                 print(f"Failed to connect to server {server}.",flush=True)
#                 shard_ids = get_shardid_given_server(None,server)
#                 print('before remove of server', list_of_servers, flush=True)
#                 list_of_servers.remove(server)
                
#                 add_response = requests.post('http://127.0.0.1:5000/add', json={'n': 1,'new_shards':[],'servers':{f'{server}':shard_ids}})
#                 if add_response.status_code == 200:
#                     print("New server added successfully.")
#                 else:
#                     print("Failed to add a new server.")
               
#         time.sleep(5)  # Wait for 5 seconds before checking again
 

if __name__ == "__main__":

    # Create a thread to run the heartbeat function
    # time.sleep(5)
    # heartbeat_thread = threading.Thread(target=heartbeat)
    # heartbeat_thread.start()
    app.run(host = "0.0.0.0",debug = True)
    