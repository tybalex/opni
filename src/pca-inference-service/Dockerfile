FROM python:3.8
COPY ./pca-inference-service/ /app/
COPY ./utils/nats_wrapper.py /app/nats_wrapper.py
COPY ./models/pca/ /app/

RUN chmod a+rwx -R /app
WORKDIR /app

RUN apt-get clean &&  apt-get update && apt-get install
RUN pip install --upgrade setuptools pip
RUN apt install -y libprotobuf-dev protobuf-compiler
RUN apt-get install -y  software-properties-common
RUN apt-get install -y build-essential -y gcc-7
RUN apt-get -y install make

RUN pip install -r requirements.txt --no-cache-dir

CMD [ "python", "window-inference-controller.py" ]
