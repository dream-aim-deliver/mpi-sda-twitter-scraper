# mpi-sda-twitter-scraper

## Description
This is a simple Twitter scraper that uses the Twitter API to scrape tweets from a given user.

## Usage
```bash
cp .env.template .env
```
### Fill in the environment variables
- API_KEY={YOUR TWITTER API KEY}
- HOST={THE HOSTNAME OF THE}
- PORT={THE PORT OF THE FASTAPI APP}

### Run the container
```bash
./run.sh
```

## Development
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python server.py
```