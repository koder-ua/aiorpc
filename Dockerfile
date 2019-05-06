FROM ubuntu:16.04
RUN apt update -y
RUN apt upgrade -y
RUN apt install -y software-properties-common git
RUN add-apt-repository -y ppa:deadsnakes/ppa
RUN apt update
RUN apt install -y python3.7 curl vim
RUN curl https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py
RUN python3.7 /tmp/get-pip.py


