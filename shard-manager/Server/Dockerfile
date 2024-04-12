
FROM flaskdocker-base
RUN apt-get update
RUN apt-get install -y \
    python3-full \
    python3-pip
RUN pip install --break-system-packages --upgrade pip
COPY . /docker-entrypoint-initdb.d/
# RUN pip install --break-system-packages -r /docker-entrypoint-initdb.d/requirements.txt


ENV MYSQL_ROOT_PASSWORD="user12"
# ENV MYSQL_USER="root"
ENV MYSQL_DATABASE="STUDENT"
