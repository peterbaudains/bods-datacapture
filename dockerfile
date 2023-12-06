FROM python:3.10

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y cron

RUN pip install pip --upgrade

RUN pip install requests && \
    pip install pandas && \
    pip install pyarrow && \
    pip install xmltodict && \
    pip install python-dotenv

COPY data_pipeline.py /usr/src/app/data_pipeline.py
COPY setup_cron.sh /usr/src/app/setup_cron.sh
RUN chmod +x /usr/src/app/setup_cron.sh
RUN (crontab -l ; echo "* * * * * /usr/local/bin/python /usr/src/app/data_pipeline.py > /proc/1/fd/1 2>/proc/1/fd/2") | crontab
