FROM mysql:8.0-debian

# COPY deploy.sh /always-initdb.d/
 #here the flask app deploy script is copied
COPY . /Server

WORKDIR /Server
# EXPOSE 5000

RUN apt-get update
RUN apt-get install -y python3
RUN  apt-get install python3-pip  -y
RUN apt install python3-venv -y
RUN python3 -m venv myenv
ENV PATH="/app/venv/bin:$PATH"

RUN myenv/bin/pip install -r  requirements.txt
# RUN myenv/bin/pip install --trusted-host pypi.python.org flask

ENV host='localhost'
ENV user='root'
ENV password='user12'


# RUN --mount=type=cache,target=/root/.cache/pip \
#     pip3 install -r requirements.txt

# RUN --mount=type=cache,target=/root/.cache/pip \
#     pip3 install -r requirements.txt




# run pip install flask
# CMD ["myenv/bin/python3","sql1.py"]