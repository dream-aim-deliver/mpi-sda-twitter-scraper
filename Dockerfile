FROM python:3.10

WORKDIR /app

COPY ./requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . /app
RUN mkdir -p  downloaded_media/photos downloaded_media/videosFROM python:3.10

WORKDIR /app

COPY ./requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . /app

RUN mkdir -p  downloaded_media/photos downloaded_media/videos