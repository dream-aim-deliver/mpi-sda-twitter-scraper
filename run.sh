#/bin/bash
DOCKER_BUILDKIT=1 docker build -t mpi-twitter-scraper .

docker run --name twitter-scraper \
    --rm \
    -e "HOST=0.0.0.0" \
    -e "PORT=8000" \
    -e "API_KEY=9ed28736eca09e2034823537c7448a35" \
    -e "OPENAI_API_KEY=~~~~~~~~~~~~~~~~" \
    -p "8000:8000" \
    mpi-twitter-scraper