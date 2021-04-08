FROM python:3.8-slim
COPY . /app/

WORKDIR /app
RUN pip install -r requirements.txt --no-cache-dir
EXPOSE 5000

# Define environment variable
ENV MODEL_NAME DrainModel
ENV SERVICE_TYPE MODEL
ENV PERSISTENCE 0

CMD exec seldon-core-microservice $MODEL_NAME --service-type $SERVICE_TYPE --persistence $PERSISTENCE
