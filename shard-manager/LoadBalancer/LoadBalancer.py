import math
import random

class ConsistentHashing:

 
    def __init__(self):

        # parameters
        self.num_slots = 512
        self.dic = {}  # to maintain server names and it's number
        self.virtual_servers = int(math.log2(self.num_slots))
        self.N = 0
        self.servers = [None]*self.num_slots
 
    def add_server(self, i):
        # add log(slots) no.of servers
        for j in range(self.virtual_servers):
            server_id = f"s_{i}_{j}"
            hash = self.server_mapping(i, j)
            self.servers[hash] = server_id
       
   
    def server_mapping(self, i, j):
 
        hash = (i**2 + j**2 + 2*j + 25)%self.num_slots
        jump = 1
        cnt = self.num_slots
        # quadratic probing is used to resolve collision
        while (self.servers[hash] != None and cnt):
            hash = (hash + jump**2) % self.num_slots
            jump += 1
            cnt -= 1

        if (cnt == 0):
            return None
        return hash % self.num_slots
 
    def remove_server(self, i):
        for j in range(self.virtual_servers):
            hash = (self.server_mapping(i, j)) % (self.num_slots)
            cnt = self.num_slots
            # same quadratic probing is used to find all virtual servers of 'i'
            while (self.servers[hash] != f"s_{i}_{j}" and cnt):
                hash = (hash + 1) % self.num_slots
                cnt -= 1
            self.servers[hash] = None
            if (cnt == 0):
                return None
       
 
    def req_server(self, req_id):
 
        hash = (self.request_mapping(req_id)) % (self.num_slots)
        cnt = self.num_slots
        while (self.servers[hash] == None and cnt):
            hash = (hash+1) % (self.num_slots)
            cnt -= 1
        if (cnt == 0):
            return None
           
        # The hash value represents the server_id of the server
        # Now extract the true server number and return it.
       
        server_num = int(self.servers[hash].split('_')[1])
        return int(server_num)
 
    def request_mapping(self, i):
        hash = i**2 + 2*i + 17
        return hash



class ShardHandle():

    # for each shard one object is maintained in the dictionary
    def __init__(self):
        self.shards = {}
        self.N = 0

    # adding the new shard
    def add_shard(self, sh_id, servers_lst):

        # if shard not present initially
        if (sh_id not in self.shards):
          self.shards[sh_id] = ConsistentHashing()
          self.N += 1

        # Iterate over the servers list and add it into respective object of shrad_id
        for i in servers_lst:
            val = self.shards[sh_id].N   # get the current number of servers in this object of shard_id
            self.shards[sh_id].add_server(val)
            self.shards[sh_id].dic[val] = i
            self.shards[sh_id].N += 1

    # to get the server with shard id
    def get_server(self, sh_id):

        uId = random.randint(10e5+1, 10e6)
        server_Id =  self.shards[sh_id].req_server(uId)
        # In the object dictionary where servers numbers are stored return the corresponding server name
        return self.shards[sh_id].dic[server_Id]

    # To remove the server in the shard
    def remove_server_in_shard(self, lst):

        for server in lst:
          for j in self.shards:

            d = self.shards[j].dic
            for key in d:
              if d[key] == server:
                self.shards[j].remove_server(key)

