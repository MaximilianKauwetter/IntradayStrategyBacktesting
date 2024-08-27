FROM python:3.11-slim-bookworm

WORKDIR /src

COPY Backtesting /src/Backtesting
COPY DataDownload /src/DataDownload
COPY main.py /src/main.py
COPY requirements.txt /src/requirements.txt

# RUN apk add gcc musl-dev linux-headers python3-dev
RUN apt-get update -y && apt-get install -y gcc  python3-dev

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python","-u","/src/main.py"]
