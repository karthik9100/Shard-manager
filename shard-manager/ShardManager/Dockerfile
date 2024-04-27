FROM flaskdocker-base
# RUN apt-get update

# RUN pip install --break-system-packages --upgrade pip
COPY . /docker-entrypoint-initdb.d/
# RUN pip install --break-system-packages -r /docker-entrypoint-initdb.d/requirements.txt
RUN chmod -R 777 /docker-entrypoint-initdb.d/

WORKDIR /docker-entrypoint-initdb.d/
ENV MYSQL_ROOT_PASSWORD="user12"
# ENV MYSQL_USER="root"
ENV MYSQL_DATABASE="STUDENT"

# CMD ["python3","docker-entrypoint-initdb.d/App.py&"]
