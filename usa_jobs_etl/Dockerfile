FROM --platform=amd64 apache/airflow:2.10.0
# copy requirements file to the docker image
COPY requirements.txt /requirements.txt
RUN python -m pip install --upgrade pip
# install python packages from requirements file
RUN pip install --no-cache-dir "apache-airflow==2.10.0" -r /requirements.txt