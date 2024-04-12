from flask import Flask
from flask import jsonify, request,make_response
import os

app = Flask(__name__)
    
#shard_id_server_id.txt 
@app.route("/primary_elect",methods = ["GET"])
def primary_election():

    try:        
       
        shard_id = request.args.get('shard_id')
        servers_list = request.args.get('server_ids').split(',')
        maxOperations = 0
        try:
            primary_server = servers_list[0]
        except:
            pass

        # for server_id in servers_list.split(','):
        #     file_name = f"{shard_id}_{server_id}.txt"
        #     fp = open(file_name, 'r')

        #     lines = fp.readlines()
        #     if lines:
        #         last_operation = lines[-1].strip()

        #     # The lines are of the format (operation_num, operation)
        #     currentOperations = int(last_operation.split()[0])
        #     if (currentOperations > maxOperations):
        #         maxOperations = currentOperations
        #         primary_server = server_id

        #     fp.close()
        return jsonify({"message": "Primary server elected", "primary_server": primary_server, "status" : "success"}), 200
        
    except Exception as e:
        return jsonify({"error": f"An error occurred in primary election: {str(e)}"}), 500


if __name__ == "__main__":
    app.run(host = "0.0.0.0",debug = True)
