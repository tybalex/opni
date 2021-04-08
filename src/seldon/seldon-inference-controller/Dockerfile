FROM python:3.8-slim
WORKDIR /code
COPY start-inference-controller.py .
COPY requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir
CMD [ "python", "start-inference-controller.py" ]
