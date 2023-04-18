#--------------------------------------------------------------------------------------
# File    :   Dockerfile
# Time    :   2023/12/04 18:38:01
# Author  :   Sindhu Chowdary Chirumamilla
# Version :   1.0
# Contact :   sindhuchowdary.chiru@dish.com
# Desc    :   This file will be used to create a docker image from code commit repo
#---------------------------------------------------------------------------------------
FROM python:3.9-slim-buster

WORKDIR /app

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/ && \
    rm -rf /var/cache/oracle-jdk8-installer;
    
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

ENTRYPOINT [ "python3", "run.py" ]