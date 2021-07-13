FROM python:slim

WORKDIR /home
COPY app .

RUN ["pip","install","-r","requirements.txt"]

ENTRYPOINT ["./start.sh"]
