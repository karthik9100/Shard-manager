FROM mysql:8.0.36-debian
RUN apt-get update
RUN apt-get install -y \
    python3-full \
    python3-pip
RUN pip install --break-system-packages --upgrade pip
COPY . /docker-entrypoint-initdb.d/

RUN pip install --break-system-packages -r /docker-entrypoint-initdb.d/requirements.txt
RUN apt-get update && apt-get install -y docker.io