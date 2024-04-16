from flask import Flask
from flask import jsonify, request,redirect,url_for,make_response
import os 
import re
import traceback
import ast
import requests
import subprocess
import LoadBalancer as lb
import Helper as hp
import uuid
import random
import threading
import mysql.connector
import threading
import time
from queue import Queue

chash = lb.ShardHandle()
operationCnt = 1

db_config = {
    
    'host': 'lbserver1',
    'user': 'root',
    'password': 'user12',
    'database': 'STUDENT',
    'port' : 3306
}


app = Flask(__name__)
app.config.from_object('config.Config')


def generate_unique_id():
    id = uuid.uuid4()  
    hash_id = hash(id)  
    return abs(hash_id) % 1000
global list_of_servers
list_of_servers = []

shard_locks = {}


class ReaderWriterLock:
    def __init__(self):
        self.lock = threading.Lock()
        self.readers_count = 0
        self.write_in_progress = False
        self.read_condition = threading.Condition(lock=self.lock)
        self.write_condition = threading.Condition(lock=self.lock)

    def acquire_read(self):
        with self.lock:
            while self.write_in_progress:
                self.read_condition.wait()
            self.readers_count += 1

    def release_read(self):
        with self.lock:
            self.readers_count -= 1
            if self.readers_count == 0:
                self.write_condition.notify()

    def acquire_write(self):
        with self.lock:
            while self.readers_count > 0 or self.write_in_progress:
                self.write_condition.wait()
            self.write_in_progress = True

    def release_write(self):
        with self.lock:
            self.write_in_progress = False
            self.read_condition.notify_all()
            self.write_condition.notify()


def initialize_locks(shard_ids):
    print(f"initializing lock vars for {shard_ids}",flush=True)
    for shard_id in shard_ids:
        shard_locks[shard_id] = ReaderWriterLock()
 

def config_shards(servers,method = ""):
    global schema
    global operationCnt
    #print("schema in config",schema,flush=True)
    config_responses={}
    config_responses = {}
    for server, server_shards in servers.items():
        config_payload = {
            "schema": schema,
            "shards": server_shards, 
            "operationCnt" : operationCnt
        }
        if(method=="add"):
            time.sleep(50)
        print("calling server config ",server,flush=True)
        config_response = requests.post(f"http://{server}:5000/config/{server}", json=config_payload).json()
        config_responses[server] = config_response
    return jsonify({"message": "Configured Database", "status": "success", "config_responses": config_responses}), 200

@app.route("/init", methods=["POST"])
def init():
    try:
        req_payload = request.json

        if 'N' in req_payload and 'schema' in req_payload and 'shards' in req_payload and 'servers' in req_payload:
            global config,schema
            config=req_payload
            N = req_payload.get('N')
            schema = req_payload.get('schema', {})
            columns = schema.get('columns', [])
            dtypes = schema.get('dtypes', [])
            shards = req_payload.get('shards', [])
            servers = req_payload.get('servers', {})
           
            connection = mysql.connector.connect(**db_config)
            

            initialize_result = hp.initialize_tables(connection)
            if 'error' in initialize_result:
                return jsonify({"error": f"An error occurred during initialization: {initialize_result['error']}"}), 500

            
            shard_insert_result = hp.insert_shard_info(connection,shards)
            if 'error' in shard_insert_result:
                return jsonify({"error": f"An error occurred during shard info insertion: {shard_insert_result['error']}"}), 500

            mapping_insert_result = hp.insert_server_shard_mapping(connection,servers)
            if 'error' in mapping_insert_result:
                return jsonify({"error": f"An error occurred during server-shard mapping insertion: {mapping_insert_result['error']}"}), 500

            shards_list_for_intializing_locks=[]
            for shard in shards:
                sid = shard['Shard_id']
                shards_list_for_intializing_locks.append(sid)
                servers_list = hp.servers_given_shard(sid, connection)
                chash.add_shard(sid, servers_list)

            new_shardId = []
            for entity in shards:
                new_shardId.append(entity['Shard_id'])

            for shard_id in new_shardId:
                for server_id in servers:
                    if shard_id in servers[server_id]:
                        updatePrimaryServer(shard_id, server_id, connection)
                        break

            print('election done', flush=True)
            printMapT(connection)
            connection.close()
            initialize_locks(shards_list_for_intializing_locks)

            print(f'calling config {servers}', flush=True)
            return config_shards(servers)

        return jsonify({"error": "Invalid payload structure"}), 400

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


def printMapT(connection):
    try:
        cursor = connection.cursor()
        select_servers_query = '''
            SELECT * FROM MapT;
        '''
        cursor.execute(select_servers_query, ())
        for row in cursor.fetchall():
            print(row, flush=True)
        connection.commit()
        cursor.close()
        
    except Exception as e:
        raise Exception(f"An error occurred in printing MapT")



@app.route('/status', methods=['GET'])
def get_status():
    global config
    
    if config is None:
        return jsonify({"error": "Database configuration not set"}), 500

    return jsonify(config), 200
        
#add servers based on the request

def copy_shard_data_to_given_server(connection,server_id,shard_id,write_server):
    try:
        config_payload = {
            "shards": [shard_id]
        }
        config_response = requests.get(f"http://{server_id}:5000/copy/{server_id}", json=config_payload).json()
        
        data_entries = config_response.get(f'{shard_id}', [])
        valid_idx=hp.get_valididx_given_shardid(connection,shard_id)
        #print(valid_idx,flush=True)
        shard_locks[shard_id].lock.acquire_write()
        config_payload2 = {
            "shard": shard_id,
            "curr_idx" : valid_idx,
            "data": data_entries["data"]
        }
        #print(f"copying data from {server_id} to {write_server}'s {shard_id}",flush=True)
        #print(f"data being copied is {config_payload2}",flush=True)
        shard_locks[shard_id].lock.acquire_write()
        config_response2 = requests.post(f"http://{write_server}:5000/write/{write_server}", json=config_payload2).json()
        
        if config_response2.get("status") == "success":
            return True, "Data copied successfully"
        else:
            return False, "Failed to copy data to the given server"

    except Exception as e:
        return False, f"An error occurred while copying data to the given server: {str(e)}"
    

@app.route('/add', methods=['POST'])
def add_servers():
    req_payload = request.json
    print("In payload ", req_payload, flush=True)
    if 'n' in req_payload and 'new_shards' in req_payload and 'servers' in req_payload:

        connection = mysql.connector.connect(**db_config)
        n = req_payload.get('n')
        new_shards = req_payload.get('new_shards', [])
        servers = req_payload.get('servers', {})

        new_shardId = []
        for entity in new_shards:
            new_shardId.append(entity['Shard_id'])

        for shard_id in new_shardId:
            #all_servers = []
            for server_id in servers:
                if shard_id in servers[server_id]:
                    updatePrimaryServer(shard_id, server_id, connection)
                    break
        
        if n > len(servers):
            return jsonify({"message": "Number of new servers (n) is greater than newly added instances", "status": "failure"}), 400
        

        k=n-len(servers)
        new_server_ids=list(servers.keys())
        pattern = r"[A-Za-z]*\[\d+\]"
        # List to store matches
        matches = []

        # Iterate over each string and find matches
        for string in new_server_ids:
            matches.extend(re.findall(pattern, string))

        print(matches)
        if(len(matches)>0):
            for match in matches:
                shard_items_of_servers = servers[match]
                del servers[match]
                index = new_server_ids.index(match)
                i1 = new_server_ids[index].find("[")
                new_server_ids[index] = new_server_ids[index][:i1]+str(generate_unique_id())
                servers[new_server_ids[index]] = shard_items_of_servers
            
        #print(new_server_ids,flush=True)

        new_shard_ids_for_intializing_locks=[]
        for dic in new_shards:
            new_shard_ids_for_intializing_locks.append(dic["Shard_id"])
        
        initialize_locks(new_shard_ids_for_intializing_locks)

        for i in new_server_ids:
            try:
                result = subprocess.run(["python3","Helper.py",str(i),"shard-manager_net1","mysqlserver","add"],stdout=subprocess.PIPE, text=True, check=True)
                
                # add the server to the list_of_servers
                list_of_servers.append(str(i))
            except Exception as e:
                msg = {
                    "message":"<Error> Unable to create some container(s),"+str(e),
                    "status" : "Faliure"
                }
                return make_response(jsonify(msg),400)
        
        try:
            try:
                config_shards(servers,"add")
            except:
                msg = {
                "message":"<Error> Unable to create shards in new servers(s)",
                    "status" : "Faliure"
                }
                return make_response(jsonify(msg),400)

            cur_shards=hp.get_shard_ids(connection)
            #print(f"getting shardids {cur_shards}",flush=True)
            for ser,shards in servers.items():
                for i in shards:
                    if i in cur_shards:
                        server_id=chash.get_server(i)
                        # print("scheduled server id in add endpoint",server_id,flush=True)
                        #print(ser,flush=True)
                        copy_shard_data_to_given_server(connection,server_id,i,ser)

            if(len(new_shards)!=0):
                shard_insert_result = hp.insert_shard_info(connection,new_shards)
                if 'error' in shard_insert_result:
                    return jsonify({"error": f"An error occurred during shard info insertion: {shard_insert_result['error']}"}), 500

            mapping_insert_result = hp.insert_server_shard_mapping(connection,servers)
            if 'error' in mapping_insert_result:
                return jsonify({"error": f"An error occurred during server-shard mapping insertion: {mapping_insert_result['error']}"}), 500

            tempdict = {}
            for server in servers:
                shard_list = servers[server]
                for shard in shard_list:
                    if shard not in tempdict:
                        tempdict[shard] = [server]
                    else:
                        tempdict[shard].append(server)

            for shard_id in tempdict:
                chash.add_shard(shard_id, tempdict[shard_id])
            
            connection.close()
            
            return jsonify({
                    "N": n,
                    "message": f"Add Server:{', '.join(new_server_ids)}",
                    "status": "success"
                }), 200

        except:
            msg = {
                "message":"<Error> Unable to create database(s)1",
                    "status" : "Faliure"
            }
            return make_response(jsonify(msg),400)
        

    return jsonify({"error": "Invalid payload structure"}), 400

#return redirect(url_for("rep"))

@app.route("/rm", methods=["DELETE"])
def remove_servers():
    print('removing servers..', flush=True)
    try:
        req_payload = request.json
        print(req_payload, flush=True)
        if 'n' in req_payload and 'servers' in req_payload:
            n = req_payload['n']
            servers_to_remove = req_payload['servers']
            print('servers to remove', servers_to_remove, flush=True)

            connection = mysql.connector.connect(**db_config)
            result = subprocess.run(["python3","Helper.py",],stdout=subprocess.PIPE, text=True, check=True)
            current_servers = result.stdout.splitlines()

            # Perform sanity checks
            if len(servers_to_remove) > n:
                return jsonify({"message": "<Error> Length of server list is more than removable instances", "status": "failure"}), 400

            elif(len(servers_to_remove)<n):
                k=n-len(servers_to_remove)
                remaining_servers = list(set(current_servers) - set(servers_to_remove))
                servers_to_remove.extend(random.sample(remaining_servers,k))

            rem_servers=len(current_servers)-n
            if(rem_servers<1):  #here <6
                msg = {
                    "message":"<Error>  Cannot remove servers as the available server count after this operation will be less than 6.",
                    "status" : "Faliure"
                }
                return make_response(jsonify(msg),400)

            # print("servers to remove",servers_to_remove,flush=True)
            for i in servers_to_remove:

                current_shards_in_server = shards_given_server(i, connection)
                for shard in current_shards_in_server:

                    if isPrimaryServer(shard, i, connection):
                        print("primary server in rm end point is ", i, flush=True)
                        servers_list = servers_given_shard(shard, connection)  # apart from current server send all the other servers
                        servers_list.remove(i)
                        
                        params = {
                            'shard_id': shard,
                            'server_ids': ','.join(str(id) for id in servers_list)
                        }
                        
                        if (len(servers_list) == 0):
                            print('Cannot remove server...', flush=True)
                            return jsonify({"message": {"No replica of servers"}, "status": "failure"}), 300
                        
                        response = requests.get(f"http://manager1:5000/primary_elect", params=params).json()
                        print('response is ', response, flush=True)
                        if (response.get('status') == "success"):
                            print("elected primary server", response.get('primary_server'),flush=True)
                            updatePrimaryServer(shard, response.get('primary_server'), connection)
                        else:
                            print('Error in primary election')
                        print('election done......', flush=True)
                try:
                    result = subprocess.run(["python3","Helper.py",str(i),"remove"],stdout=subprocess.PIPE, text=True, check=True)
                    #implement hashing
                    # obj.remove_server(obj.dic[i])
                    list_of_servers.remove(i)
                except:
                    msg = {
                        "message":"<Error>  Unable to remove some container(s)",
                        "status" : "Faliure"
                    }
                    return make_response(jsonify(msg),400)

            hp.update_shardt_mapt_tables(connection,servers_to_remove)
            connection.close()
            chash.remove_server_in_shard(servers_to_remove)
            return jsonify({"message": {"N": len(servers_to_remove), "servers":servers_to_remove}, "status": "successful"}), 200

        return jsonify({"error": "Invalid payload structure"}), 400

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500



@app.route("/read", methods=["POST"])
def reading_data():
    global list_of_servers
    print('after remove servers', list_of_servers, flush=True)
    try:
        req_payload = request.json

        if 'Stud_id' in req_payload:
            stud_id_range = req_payload['Stud_id']
            low = stud_id_range['low']
            high = stud_id_range['high']

            connection = mysql.connector.connect(**db_config)            
            shards_queried = hp.get_queried_shards_with_ranges(connection,low, high)
            data=[]
            keys=[]
            #multiple reads should be allowed
            for item in shards_queried:
                shardid=item["Shard_id"]
                keys.append(shardid)
                servers_shard=hp.servers_given_shard(shardid,connection)
                mapping_ser=chash.get_server(shardid)
                print("print mapping_server", mapping_ser, flush=True)
                # print("read endpoint load balancer,mapped server id ",mapping_ser,flush=True)
                # mapping_serverid=req_payload['server_id']  ###### consistent hashing
                
                data.extend(read_from_shard(connection,shardid,mapping_ser,item["Ranges"]))

            connection.close()

            return jsonify({"shards_queried": keys, "data": data, "status": "success"}), 200

        return jsonify({"error": "Invalid payload structure"}), 400

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


def servers_given_shard(shard,connection):
    #connection = mysql.connector.connect(**db_config) 
    try:
        cursor = connection.cursor()
        select_servers_query = '''
            SELECT Server_id FROM MapT WHERE Shard_id = %s;
        '''
        cursor.execute(select_servers_query, (shard,))
        server_ids = [row[0] for row in cursor.fetchall()]
        connection.commit()
        cursor.close()
        return server_ids
    except Exception as e:
        raise Exception(f"An error occurred while retrieving Server IDs for Shard {shard}: {str(e)}")


def write_to_shard(connection, shard_id, shard_data):
    global operationCnt
    #operationCnt+=1
    connection = mysql.connector.connect(**db_config) 
    try:
        if shard_id not in shard_locks:
            shard_locks[shard_id] = ReaderWriterLock()
        shard_locks[shard_id].acquire_write()
        s=[]
        servers_list = servers_given_shard(shard_id, connection)

        isPrimaryDict = {}
        primary_server = None
        for server in servers_list:
            flag = isPrimaryServer(shard_id, server, connection)
            if flag:
                primary_server = server
            isPrimaryDict[server] = flag

        # print(f" printing servers list in write_to_shard {servers_list} in {shard_id}",flush=True)
       
        config_payload = {
            "shard": shard_id,
            "curr_idx": shard_data['valid_idx'],
            "data": shard_data['entries'],
            "servers": isPrimaryDict,
            "id" : operationCnt
        }
        print(f"in write endpoint {primary_server}",flush=True)
        
        config_response = requests.post(f"http://{primary_server}:5000/write/{primary_server}", json=config_payload).json()
        print("in write endpoint1",config_response, flush=True)
        operationCnt += 1

    except Exception as e:
        print(f"An error occurred while writing to shard {shard_id} on server {server}: {str(e)}")
        traceback.print_exc()
    finally:
        shard_locks[shard_id].release_write()
        print("write done on all shards and releasing the locks",s,flush=True)


@app.route('/write', methods=['POST'])
def write_data_load_balancer():

    try:
        request_payload = request.json
        data_entries = request_payload.get('data')
        if data_entries and isinstance(data_entries, list):
            connection = mysql.connector.connect(**db_config) 
            ind_shard_data = hp.get_shard_ids_corresponding_write_operations(connection, data_entries)
            threads = []
            for shard_id, shard_data in ind_shard_data.items():
                write_to_shard(connection, shard_id, shard_data)
                # thread = threading.Thread(target=write_to_shard, args=(connection,shard_id,shard_data))
                # threads.append(thread)
                # thread.start()

            # for thread in threads:
            #     thread.join()  # Wait for all threads to complete

            connection.close()
            return jsonify({"message": f"{len(data_entries)} Data entries added", "status": "success"}), 200
        else:
            return jsonify({"error": "Invalid payload structure"}), 400
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

result_queue_for_update = Queue()

def update_to_shard(config_payload,shard_id,server_id):
    try:
        if shard_id not in shard_locks:
            shard_locks[shard_id] = ReaderWriterLock()

        shard_locks[shard_id].acquire_write()
        config_response = requests.put(f"http://{server_id}:5000/update/{server_id}", json=config_payload).json()
        result_queue_for_update.put(config_response)
        
    except Exception as e:
        print(f"An error occurred while updating to shard {shard_id} on server {server_id}: {str(e)}")
        traceback.print_exc()
    finally:
        shard_locks[shard_id].release_write()

@app.route('/update', methods=['PUT'])
def update_student_info():
    global operationCnt
    try:
        req_payload = request.json
        if 'Stud_id' in req_payload and 'data' in req_payload:
 
            stud_id = req_payload['Stud_id']
            data = req_payload['data']
            connection = mysql.connector.connect(**db_config)
            shard_id = hp.get_shard_id_by_stud_id(connection, stud_id)
            servers_list = servers_given_shard(shard_id, connection)
            # threads = []
 
            isPrimaryDict = {}
            for server in servers_list:
                flag = isPrimaryServer(shard_id, server, connection)
                isPrimaryDict[server] = flag
 
            for server_id in servers_list:
 
                if (isPrimaryServer(shard_id, server_id, connection)):
 
                    config_payload = {
                        "shard": shard_id,
                        "Stud_id" : stud_id,
                        "data" : data,
                        "servers": isPrimaryDict,
                        "id" : operationCnt
                    }
                    update_to_shard(config_payload, shard_id, server_id)
                    break
               
            operationCnt += 1
            #     thread = threading.Thread(target=update_to_shard, args=(config_payload,shard_id,server_id))
            #     threads.append(thread)
            #     thread.start()
 
            # for thread in threads:
            #     thread.join()
 
            res=result_queue_for_update.get()
            if(res["status"]=="not_found"):
                return res
            return jsonify({"message": f"Data entry for Stud_id: {stud_id} updated",
                            "status" : "success"}
                            ), 200
 
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500
    finally:
        if connection and connection.is_connected():
            connection.close()


result_queue_for_delete = Queue()

def delete_from_shard(config_payload,shard_id,server_id):
    try:
        if shard_id not in shard_locks:
            shard_locks[shard_id] = ReaderWriterLock()

        shard_locks[shard_id].acquire_write()
        config_response = requests.delete(f"http://{server_id}:5000/del/{server_id}", json=config_payload).json()
        result_queue_for_delete.put(config_response)

    except Exception as e:
        print(f"An error occurred while updating to shard {shard_id} on server {server_id}: {str(e)}")
        traceback.print_exc()
    finally:
        shard_locks[shard_id].release_write()

@app.route('/del', methods=['DELETE'])
def remove_student_info():
    
    global operationCnt
    try:
        req_payload = request.json
        if 'Stud_id' in req_payload:
           
            stud_id = req_payload['Stud_id']
            connection = mysql.connector.connect(**db_config)
            shard_id = hp.get_shard_id_by_stud_id(connection, stud_id)
            servers_list = hp.servers_given_shard(shard_id, connection)
            threads = []
 
            isPrimaryDict = {}
            for server in servers_list:
                flag = isPrimaryServer(shard_id, server, connection)
                isPrimaryDict[server] = flag
 
            for server_id in servers_list:
 
                if (isPrimaryServer(shard_id, server_id, connection)):
                    config_payload = {
                        "shard": shard_id,
                        "Stud_id" : stud_id,
                        "servers": isPrimaryDict,
                        "id" : operationCnt
                    }
                    delete_from_shard(config_payload, shard_id, server_id)
                    break
               
            operationCnt += 1
 
            #     thread = threading.Thread(target=delete_from_shard, args=(config_payload,shard_id,server_id))
            #     threads.append(thread)
            #     thread.start()
 
            # for thread in threads:
            #     thread.join()
           
            res=result_queue_for_delete.get()
            if(res["status"]=="not_found"):
                return res
            return jsonify({"message": f"Data entry for Stud_id: {stud_id} deleted",
                            "status" : "success"}
                            ), 200
       
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500
    finally:
        if connection and connection.is_connected():
            connection.close()

# routes requests to one of the avaliable servers

@app.route("/<path>",methods = ["GET"])
def pathRoute1(path):
    if(path!="home"):
        msg = {
            "message":"<Error> Page Not Found",
            "status" : "Faliure"
            }
        return make_response(jsonify(msg),400)
    #assigning uuid to each request
    max_value = 10**(6) - 1
    request_id = uuid.uuid4().int % max_value
    temp = 512
    while(temp>=0):
        server_id = obj.req_server(request_id)
        # if all the servers goes down then 3 new servers will spawn
        if (server_id == None):
            obj.N+=1
            server_name = "Sa1wasd"+str(obj.N)
            obj.dic[server_name] = obj.N
            result = subprocess.run(["python","Helper.py",server_name,"shard-manager_net1","flaskserver1","add"],stdout=subprocess.PIPE, text=True, check=True)
            obj.add_server(obj.dic[server_name])
            continue
        
        for i,j in obj.dic.items():
            if(j==server_id):
                server_name = i
                break
        
        #checkheartbeat
        
        try:
            res = requests.get(f'http://{server_name}:5000/heartbeat')
            if(res.status_code==200):
                break 
        except Exception as errh:
           
            result = subprocess.run(["python","Helper.py",server_name,"remove"],stdout=subprocess.PIPE, text=True, check=True)
            obj.remove_server(obj.dic[server_name])
            obj.N+=1
            server_name = "Sa1wasd"+str(obj.N)
            obj.dic[server_name] = obj.N
            result = subprocess.run(["python3","Helper.py",server_name,"shard-manager_net1","mysqlserver","add"],stdout=subprocess.PIPE, text=True, check=True)
            obj.add_server(obj.dic[server_name])
            

        temp-=1
    response = requests.get(f'http://{server_name}:5000/home/{server_name}')
    return make_response(jsonify(response.json()),200)

def shards_given_server(server_id,connection):
    #connection = mysql.connector.connect(**db_config) 
    try:
        cursor = connection.cursor()
        select_servers_query = '''
            SELECT Shard_id FROM MapT WHERE Server_id = %s;
        '''
        cursor.execute(select_servers_query, (server_id,))
        shard_ids = [row[0] for row in cursor.fetchall()]
        connection.commit()
        cursor.close()
        return shard_ids
    except Exception as e:
        raise Exception(f"An error occurred while retrieving Shard IDs for Server {server_id}: {str(e)}")

def updatePrimaryServer(shard_id, server_id, connection):
     
    try:
        cursor = connection.cursor()
        select_servers_query = '''
            UPDATE MapT SET IsPrimary = 1 WHERE Server_id = %s AND Shard_id = %s;
        '''
        cursor.execute(select_servers_query, (server_id, shard_id))
        #flag = len([row[0] for row in cursor.fetchall()])
        connection.commit()
        cursor.close()
        
    except Exception as e:
        raise Exception(f"An error occurred while retrieving Shard IDs for Server {server_id}: {str(e)}")


@app.route('/getServerList', methods=['GET'])
def getServerList():
    print('server list in heartbeat', flush=True)
    return jsonify({"servers": list_of_servers, "status" : "success"}), 200
    
@app.route('/getShardIdGivenServer', methods=['GET'])
def getShardIdGivenServer():

    print('shard ids in heartbeat', flush=True)
    server = request.args.get('server')
   
    shard_ids = get_shardid_given_server(None,server)
    return jsonify({"shard_ids": shard_ids, "status" : "success"}), 200


def isPrimaryServer(shard_id, server_id, connection):
    
    try:
        cursor = connection.cursor()
        select_servers_query = '''
            SELECT Server_id FROM MapT WHERE Server_id = %s AND Shard_id = %s AND IsPrimary = 1;
        '''
        cursor.execute(select_servers_query, (server_id, shard_id))
        ans = [row[0] for row in cursor.fetchall()]
        flag = len(ans) > 0
        print(f" flag = {flag} and ans = {ans} is primary server call",list(cursor.fetchall()),flush=True)
        connection.commit()
        cursor.close()
        
        if flag:
            return 1
        return 0
    except Exception as e:
        raise Exception(f"An error occurred while retrieving Shard IDs for Server {server_id}: {str(e)}")


# returns list of shardid corresponding to a server
def get_shardid_given_server(connection,server):
    if (connection is None):
        connection = mysql.connector.connect(**db_config)
    try:
        cursor = connection.cursor()
        query = f"SELECT Shard_Id from MapT where server_id = '{server}';"
        cursor.execute(query)
        result = cursor.fetchall()
        shard_ids = [row[0] for row in result]
        return shard_ids

    except Exception as e:
        print(f"An error occurred while fetching shard ids: {str(e)}")

    finally:
        connection.commit()
        cursor.close()


@app.route("/helloread", methods=["POST"])
def helloread():
    try:
        req_payload = request.json
        server = req_payload['server']
        shard = req_payload['shard']
        if not server or not shard:
            msg = {
                "message": "Filename is required in the payload",
                "status": "Failed"
            }
            return make_response(jsonify(msg), 400)
           
        response = requests.post(f'http://{server}:5000/readlog', json={"filename": f"{shard}_{server}.txt"})
        return jsonify({
            "message": response.json().get("message"),
            "status": response.status_code
        }), response.status_code
    except Exception as e:
        return jsonify({
            "message": str(e),
            "status": 500
        }), 500

@app.route("/hellowrite/<server_name>", methods=["GET"])
def hellowrite(server_name):
    try:
        response = requests.post(f'http://{server_name}:5000/writelog', json={"filename": server_name, "content": f"Hello from load balancer {server_name}"})
        return jsonify({
            "message": response.json().get("message"),
            "status": response.status_code
        }), response.status_code
    except Exception as e:
        return jsonify({
            "message": str(e),
            "status": 500
        }), 500

# continuously check heartbeat
# Define the heartbeat function



@app.errorhandler(404)

def errorPage(k):
    return "Page not found"



if __name__ == "__main__":
    # 6 replicas of server are maintained
    for i in ["Server0","Server2", "Server3"]:#,"Server2","Server3","Server4","Server5", 'Server6']:
        try:
            result = subprocess.run(["python3","Helper.py",str(i),"shard-manager_net1","mysqlserver","add"],stdout=subprocess.PIPE, text=True, check=True)
        except Exception as e:
            # pass
            print("error",e)
        list_of_servers.append(i)


    
    app.run(host = "0.0.0.0",debug = True)
