FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY YacloudS3.py YacloudS3.py
COPY MOEXBondScrinner.py MOEXBondScrinner.py

CMD [ "python3", "./YacloudS3.py"]
