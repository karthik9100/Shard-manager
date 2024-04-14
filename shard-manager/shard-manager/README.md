# Sharding
# Implementing Load Balancer 

_Load Balancer is a server that routes all the incoming requests to the available servers, so that the load is distributed evenly across all the servers. It also increases or decreases the number of servers based on the requirement. Thus availability is ensured._



-----------------------------------------------------------------------------------------------------------------------------
# Prerequisites
**1 .Docker version 24.0.7, build afdd53b**

sudo apt-get update

sudo apt-get install ca-certificates curl gnupg lsb-release

sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
$(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update

sudo apt-get install docker-ce docker-ce-cli containerd.io

-----------------------------------------------------------------------------------------------------------------------------

**2. Docker Compose **

sudo curl -SL https://github.com/docker/compose/releases/download/v2.15.1/docker-compose-linux-x86_64 -o /usr/local/bin/docker-compose

sudo chmod +x /usr/local/bin/docker-compose

sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose


# How to run the application

1. make run
   
    --> this runs 'docker compose up' run command, which will build images for both the loadbalancer and flaskserver, and one container for each of them (lbserver1, server1)
   
    --> this will run in a detached mode. To exit press ctrl+c
   
2. make stop
   
    --> this command removes all the containers and images created previously
   
    --> ## It is recommended to run this command for smooth working of the application ##
   
# Logic
   ## Load Balancer
   Load balancer contains methods required for server mapping, request server etc.
   It has class which is initialized with number of slots, number of virtual servers, server mapping datastructure ( python list )
   python list is maintained as circular array for consistent hashing.

   --> add server
   
   To add server we find the server mapping(i, j) where i is the server number, j is the virtual number of that server.
   We repeat for virtual number of server times and mapped that virtual server into server circular array.

   --> server mapping
   
   In server mapping a hash function i**2 + j**2 + 2*j + 25 is used for server map.
   If there is a collision then it is resolved by using linear probing.

   --> remove server
   
   In this method all the virtual servers of given server is removed by traversing the circular array k times
   where k is the number of virtual servers.

   --> request server
   
   In this method the request has to be assigned a server .
   so we use a hash function i**2 + 2*i + 27 to map to server circular table.
   After getting the hash value we traverse circular array in clockwise until we get a server id.
   And finally return the original server number to assign that server.


   ## App.py
      -> It has 8 endpoints (add, rem, rep, <path>)
      
      -> add 
         is post request which accepts two parameters n, replicas
         you can send this request using  ** POSTMAN ** using the below request of POST method
                     http://127.0.0.1:5000/add?n=1&replicas=['s21']
                     if n > len(replicas) then n-len(replicas) number of random servers are created along with ones mentioned in the request
                     
      -> rm
         is post request which accepts two parameters n, replicas
         you can send this request using  ** POSTMAN ** using the below request of POST method
                     http://127.0.0.1:5000/rm?n=1&replicas=['s21']
                     if n > len(replicas) then n-len(replicas) number of random servers are removed along with ones mentioned in the request

      -> rep 
         is a GET request
         you can send this request using  ** POSTMAN ** using the below request of GET method
                     http://127.0.0.1:5000/rep
                     returns all available servers


