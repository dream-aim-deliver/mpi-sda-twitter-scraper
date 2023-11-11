# Stage 1: Build stage
FROM python:3.10 AS builder

WORKDIR /twitter_api_scraper

COPY . /twitter_api_scraper

RUN python3 -m venv .venv
ENV PATH="/twitter_api_scraper/.venv/bin:$PATH"
RUN pip install --upgrade pip && pip install -r requirements.txt

# Stage 2: Run stage
FROM python:3.10-slim

ENV PORT=8000

WORKDIR /twitter_api_scraper

COPY --from=builder /twitter_api_scraper /twitter_api_scraper
COPY --from=builder /twitter_api_scraper/.venv /twitter_api_scraper/.venv

ENV PATH="/twitter_api_scraper/.venv/bin:$PATH"

EXPOSE ${PORT}
CMD ["python", "server.py"]
