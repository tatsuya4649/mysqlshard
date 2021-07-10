FROM python:slim

WORKDIR /home
COPY . .
COPY ../nodes .

RUN ["pip","install","-r","requirements.txt"]

ENTRYPOINT ["python3","main.py"]
