FROM python:3.9

ENV C_FORCE_ROOT true

WORKDIR /rq

COPY requirements.txt /rq

RUN pip install -r requirements.txt
RUN apt-get install curl

COPY . /rq

# Should publish latest version of csv-detective and add it to requirements
RUN wget https://github.com/etalab/csv-detective/archive/refs/tags/0.4.1.zip
RUN unzip 0.4.1.zip
RUN mv csv-detective-0.4.1/csv_detective/ .
RUN rm -rf csv-detective-0.4.1
RUN rm -rf 0.4.1.zip