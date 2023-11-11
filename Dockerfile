FROM python:3.10

# ARG TAG=latest

ENV PORT=8000

WORKDIR /twitter_api_scraper

COPY . /twitter_api_scraper

RUN pip install -r requirements.txt

RUN mkdir -p downloaded_media/tweet && mkdir -p downloaded_media/link

EXPOSE ${PORT}

CMD ["python", "server.py"]
