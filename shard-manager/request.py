import asyncio
import aiohttp
import time, random, uuid
 
async def make_request(session, method, url, payload):

    if (method == 'POST'):
        async with session.post(url, json = payload) as response:
            return await response.text()

def generate_unique_id():
    generated_uuid = uuid.uuid4()
    # Convert UUID to hex string without hyphens and take the first 6 characters
    hex_str = generated_uuid.hex[:6]
    # Convert hex string to int
    random_id = int(hex_str, 16) % 12000  # Ensure it's within 6 digits
    return '{:06d}'.format(random_id)  # Ensure the ID is of length 6 by padding with zeros if necessary


def generate_name():
    name = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=6))
    return name

async def send_10k_read_req(no_of_requests):
    
    url_read = "http://127.0.0.1:5000/read"
    cnt = 0
    start_time, read_time = time.time(), 0
    while (cnt < no_of_requests):

        high = random.randint(0, 16000)
        low = random.randint(0, high)
        read_payload = {"Stud_id": {"low":low, "high":high}}

        try:
            async with aiohttp.ClientSession() as session:
                tasks = [make_request(session, 'POST', url_read, payload = read_payload)]
                responses = await asyncio.gather(*tasks)

            end_time = time.time()
            read_time += (end_time - start_time)
            start_time = end_time

            # To see the data of students records
            # for res in responses:
            #     print(json.loads(res)["data"])
        
        except:
            pass
        cnt += 1

    print(f'read speed for {no_of_requests} requests: ', read_time)


async def send_10k_write_req(no_of_requests):
    lis = []
    url_write = "http://127.0.0.1:5000/write"
    cnt, existing_ids = 0, set()
    start_time = time.time()
    while (cnt < no_of_requests):
        n = random.randint(1, 3)
        temp, write_payload = {}, {}
        write_payload['data'] = []

        for i in range(n):
            sid = generate_unique_id()
            #existing_ids.add(sid)
            lis.append(sid)
            sname = generate_name()
            smarks = random.randint(0, 101)

            temp['Stud_id'] = sid
            temp['Stud_marks'] = smarks
            temp['Stud_name'] = sname
            
            write_payload['data'].append(temp.copy())
            temp.clear()
 
        async with aiohttp.ClientSession() as session:
            tasks = [make_request(session, 'POST', url_write, payload = write_payload)]
            responses = await asyncio.gather(*tasks)
        cnt += 1

    write_time = time.time() - start_time
    print(f'write speed for {no_of_requests} requests: ', write_time)
    # print("lis = ",lis,"length is ",len(lis))


async def main():

    no_of_requests = 1000
    await send_10k_write_req(no_of_requests)
    await send_10k_read_req(no_of_requests)

    
 
if __name__ == "__main__":
    asyncio.run(main())
