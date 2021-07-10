FROM python:slim

WORKDIR /home
COPY . .

RUN ["pip","install","-r","requirements.txt"]

ENTRYPOINT ["python3","main.py"]
