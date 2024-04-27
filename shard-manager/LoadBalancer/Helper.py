import subprocess
import sys
import docker 
def main1(params):
   
    cmd = ["docker", "run", "--name", params[1],"--network", params[2], "--network-alias", params[1], "-d","-v", "shareddata:/docker-entrypoint-initdb.d/",params[3]]
    
    try:
        result = subprocess.run(cmd)
        print("Return Code:", result.returncode)
        if(result.returncode!=0):
            print("Container not created")
       
    except Exception as e:
        print("Container not created")

def main2(params):
   
    cmd = ["docker", "rm", "-f", params[1]]
    
    try:
        result = subprocess.run(cmd)
        print("Return Code:", result.returncode)
        if(result.returncode!=0):
            raise Exception("Container not removed")
       
    except Exception as e:
        raise Exception("Container not removed")

def get_docker_processes():
    try:
        # Run the docker ps command with custom formatting
        result = subprocess.run(["docker", "ps","--filter", "ancestor=mysqlserver", "--format",'{{.Names}}'], capture_output=True, text=True, check=True)

        container_names = result.stdout.splitlines()
        for i in container_names:
            print(i)

    except subprocess.CalledProcessError as e:
        print(f"Error running 'docker ps' command: {e.stderr}")



def initialize_tables(connection):
    try:
        cursor = connection.cursor()

        # Initialize ShardT table
        cursor.execute('DROP TABLE IF EXISTS ShardT')
        create_shardt_table_query = '''
            CREATE TABLE IF NOT EXISTS ShardT (
                Stud_id_low INT PRIMARY KEY,
                Shard_id VARCHAR(255),
                Shard_size INT,
                valid_idx INT
            );
        '''
        cursor.execute(create_shardt_table_query)

        # Initialize MapT table
        cursor.execute('DROP TABLE IF EXISTS MapT')
        create_mapt_table_query = '''
            CREATE TABLE IF NOT EXISTS MapT (
                Shard_id VARCHAR(255),
                Server_id VARCHAR(255),
                IsPrimary INT DEFAULT 0
            );
        '''
        cursor.execute(create_mapt_table_query)

        connection.commit()
        cursor.close()

    except Exception as e:
        return {"error": f"An error occurred while initializing tables: {str(e)}"}

    return {"message": "Tables initialized successfully"}


def insert_shard_info(connection,shards):
    try:
        cursor = connection.cursor()

        for shard_info in shards:
            insert_shard_query = '''
                INSERT INTO ShardT (Stud_id_low, Shard_id, Shard_size, valid_idx)
                VALUES (%s, %s, %s, %s)
            '''
            cursor.execute(insert_shard_query, (
                shard_info['Stud_id_low'],
                shard_info['Shard_id'],
                shard_info['Shard_size'],
                shard_info['Stud_id_low']
            ))

        connection.commit()
        cursor.close()
        
    except Exception as e:
        return {"error": f"An error occurred while inserting shard info: {str(e)}"}

    return {"message": "Shard info inserted successfully"}


def insert_server_shard_mapping(connection,servers):
    try:
        #if connection is not None:
        cursor = connection.cursor()

        for server_id, shard_ids in servers.items():
            for shard_id in shard_ids:
                insert_mapping_query = '''
                    INSERT INTO MapT (Shard_id, Server_id)
                    VALUES (%s, %s)
                '''
                cursor.execute(insert_mapping_query, (shard_id, server_id))

        connection.commit()
        cursor.close()

    except Exception as e:
        return {"error": f"An error occurred while inserting server-shard mapping: {str(e)}"}

    return {"message": "Server-shard mapping inserted successfully"}

def get_shard_ids(connection):
    try:
        cursor = connection.cursor()
        select_shard_ids_query = '''
            SELECT DISTINCT Shard_id FROM ShardT;
        '''
        cursor.execute(select_shard_ids_query)
        shard_ids = [row[0] for row in cursor.fetchall()]

        connection.commit()
        cursor.close()
        return shard_ids
    except Exception as e:
        raise Exception(f"An error occurred while retrieving Shard IDs: {str(e)}")

def servers_given_shard(shard,connection):
    print("Helper ", shard,flush=True)
    try:
        cursor = connection.cursor()
        select_servers_query = '''
            SELECT Server_id FROM MapT WHERE Shard_id = %s;
        '''
        cursor.execute(select_servers_query, (shard,))
        server_ids = [row[0] for row in cursor.fetchall()]
        connection.commit()
        cursor.close()
        print("Helper ", server_ids,flush=True)
        return server_ids
    except Exception as e:
        raise Exception(f"An error occurred while retrieving Server IDs for Shard {shard}: {str(e)}")

def update_shardt_mapt_tables(connection,servers_to_remove):
    cursor = connection.cursor()
    for i in servers_to_remove:
        cursor.execute('DELETE FROM MapT WHERE Server_id = %s', (i,))
        print(f"deleted {i} record successfully", flush=True)   
    connection.commit()
    cursor.close()


def get_queried_shards_with_ranges(connection, low, high):
    queried_shards = []
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute('SELECT Shard_id, Stud_id_low, Stud_id_low + Shard_size AS Stud_id_high FROM ShardT WHERE Stud_id_low <= %s AND Stud_id_low + Shard_size >= %s', (high, low))
        result = cursor.fetchall()
        if result:
            for row in result:
                shard_id = row['Shard_id']
                stud_id_low = row['Stud_id_low']
                stud_id_high = row['Stud_id_high']

                ranges_within_shard = {}
                ranges_within_shard["low"]=(max(stud_id_low, low))
                ranges_within_shard["high"]=(min(stud_id_high, high))
                queried_shards.append({"Shard_id": shard_id, "Ranges": ranges_within_shard})
        cursor.close()

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    return queried_shards

def get_shard_ids_corresponding_write_operations(connection, entries):
    try:
        cursor = connection.cursor()
        shard_data = {}
        query = "SELECT Shard_id, valid_idx FROM ShardT WHERE Stud_id_low <= %s AND (Stud_id_low + Shard_size) > %s"

        for entry in entries:
            stud_id = entry['Stud_id']
            cursor.execute(query, (stud_id, stud_id))
            result = cursor.fetchone()
            if result:
                shard_id, valid_idx = result
                if shard_id in shard_data.keys():
                    shard_data[shard_id]['entries'].append(entry)
                else:
                    shard_data[shard_id] = {'valid_idx': valid_idx, 'entries': [entry]}

        return shard_data

    except Exception as e:
        print(f"An error occurred while fetching shard ids: {str(e)}")

    finally:
        connection.commit()
        cursor.close()

def get_valididx_given_shardid(connection,shard_id):
    try:
        cursor = connection.cursor()
        query = "SELECT valid_idx FROM ShardT WHERE Shard_id = %s"
        cursor.execute(query, (shard_id,))
        result = cursor.fetchone()
        if result:
            return result[0]

    except Exception as e:
        print(f"An error occurred while fetching shard ids: {str(e)}")

    finally:
        connection.commit()
        cursor.close()


def get_shard_id_by_stud_id(connection, stud_id):
    try:
        cursor = connection.cursor()
        query = "SELECT Shard_id FROM ShardT WHERE Stud_id_low <= %s AND (Stud_id_low + Shard_size) > %s"
        cursor.execute(query, (stud_id, stud_id))
        result = cursor.fetchone()
        if result:
            return result[0]
    except Exception as e:
        print(f"An error occurred while fetching shard_id: {str(e)}")

    finally:
        connection.commit()
        cursor.close()
    return None

    
if __name__ == "__main__":
    # print(sys.argv)
    # print(">>>>>>>>>>>>>> IN HELPER <<<<<<<<<<<")
    if(sys.argv[-1]=="add"):
        main1(sys.argv)
    elif(sys.argv[-1]=="remove"):
        main2(sys.argv)
    else:
        get_docker_processes()


