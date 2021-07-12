FROM python:slim

WORKDIR /home
COPY app .
COPY nodes ./nodes/

RUN ["pip","install","-r","requirements.txt"]

ENTRYPOINT ["./start.sh"]
