FROM python:3.8
COPY . /app/

RUN chmod a+rwx -R /app
WORKDIR /app

RUN apt-get clean &&  apt-get update && apt-get install
RUN pip install --upgrade setuptools pip
RUN apt install -y libprotobuf-dev protobuf-compiler
RUN apt-get install -y  software-properties-common
RUN apt-get install -y build-essential -y gcc-7
RUN apt-get -y install make

RUN pip install -r requirements.txt --no-cache-dir
EXPOSE 5000

# Define environment variable
ENV MODEL_NAME PcaModel
ENV SERVICE_TYPE MODEL
ENV PERSISTENCE 0

CMD exec seldon-core-microservice $MODEL_NAME --service-type $SERVICE_TYPE --persistence $PERSISTENCE
