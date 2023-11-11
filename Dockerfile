# Stage 1: Build stage
FROM python:3.10 AS builder

WORKDIR /scaper

COPY . /scaper

RUN python3 -m venv .venv
ENV PATH="/scaper/.venv/bin:$PATH"
RUN pip install --upgrade pip && pip install -r requirements.txt

# Stage 2: Run stage
FROM python:3.10-slim

ENV PORT=8000

WORKDIR /scaper

COPY --from=builder /scaper /scaper
COPY --from=builder /scaper/.venv /scaper/.venv

ENV PATH="/scaper/.venv/bin:$PATH"

EXPOSE ${PORT}
CMD ["python", "server.py"]
