FROM python:3.11-alpine

WORKDIR /src

COPY Backtesting /src/Backtesting
COPY DataDownload /src/DataDownload
COPY main.py /src/main.py
COPY requirements.txt /src/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python","/src/main.py"]
