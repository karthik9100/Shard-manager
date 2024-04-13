
FROM serverbaseimg
COPY . /docker-entrypoint-initdb.d/
RUN pip install --break-system-packages -r /docker-entrypoint-initdb.d/requirements.txt
RUN chmod 777  /docker-entrypoint-initdb.d/


ENV MYSQL_ROOT_PASSWORD="user12"
# ENV MYSQL_USER="root"
ENV MYSQL_DATABASE="STUDENT"
