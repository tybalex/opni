# Standard Library
import asyncio
import json
import logging
import os
import time

# Third Party
import boto3
from botocore.config import Config
from nats.aio.client import Client as NATS

MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
NATS_SERVER_URL = os.environ["NATS_SERVER_URL"]


async def run(loop, queue, nc):
    async def error_cb(e):
        logging.warning("Error: {}".format(str(e)))

    async def closed_cb():
        logging.warning("Closed connection to NATS")
        await asyncio.sleep(0.1, loop=loop)
        loop.stop()

    async def on_disconnect():
        logging.warning("Disconnected from NATS")

    async def reconnected_cb():
        logging.warning(
            "Reconnected to NATS at nats://{}".format(nats.connected_url.netloc)
        )

    async def subscribe_handler(msg):
        subject = msg.subject
        reply = msg.reply
        payload_data = msg.data.decode()
        logging.info("Received log messages.")
        await queue.put(payload_data)

    options = {
        "loop": loop,
        "error_cb": error_cb,
        "closed_cb": closed_cb,
        "reconnected_cb": reconnected_cb,
        "disconnected_cb": on_disconnect,
        "servers": [NATS_SERVER_URL],
    }

    try:
        await nc.connect(**options)
    except Exception as e:
        logging.error(str(e))

    logging.info(f"Connected to NATS at {nc.connected_url.netloc}...")

    await nc.subscribe("model_ready", "", subscribe_handler)


class MyModel:
    def __init__(self, method: str = "predict"):
        self.method = method
        self.to_reload_model = False
        self.download_from_minio()
        # asyncio.get_child_watcher()
        # loop = asyncio.new_event_loop()
        # nats_thread = threading.Thread(target = self.setup_nats, args=(loop,))
        # nats_thread.daemon = True
        # nats_thread.start()

    async def wait_for_payload(self, queue, nc):
        while True:
            payload = await queue.get()
            if payload is None:
                break
            logging.info(payload)
            try:
                decoded_payload = json.loads(payload)
                # process the payload
                if "bucket" in decoded_payload:
                    if decoded_payload["bucket"] == "nulog-models":
                        logging.info(
                            "Just received signal to download a new Nulog model files from Minio."
                        )
                        self.download_from_minio(decoded_payload)
                        self.to_reload_model = True
            except Exception as e:
                logging.error(e)

    def setup_nats(self, loop):
        nc = NATS()
        asyncio.set_event_loop(loop)
        queue = asyncio.Queue(loop=loop)
        run_jobs_coroutine = self.wait_for_payload(queue, nc)
        consumer_coroutine = run(loop, queue, nc)
        loop.run_until_complete(asyncio.gather(run_jobs_coroutine, consumer_coroutine))
        try:
            loop.run_forever()
        finally:
            loop.close()

    def download_from_minio(
        self,
        decoded_payload={
            "bucket": "nulog-models",
            "bucket_files": {
                "model_file": "nulog_model_latest.pt",
                "vocab_file": "vocab.txt",
            },
        },
    ):

        endpoint_url = "http://minio.default.svc.cluster.local:9000"
        minio_client = boto3.resource(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
        )
        if not os.path.exists("output/"):
            os.makedirs("output")

        bucket_name = decoded_payload["bucket"]
        bucket_files = decoded_payload["bucket_files"]
        for k in bucket_files:
            try:
                minio_client.meta.client.download_file(
                    bucket_name, bucket_files[k], "output/{}".format(bucket_files[k])
                )
            except Exception as e:
                logging.error(
                    "Cannot currently obtain necessary model files. Exiting function"
                )
                return

    def load(self):
        # Third Party
        import inference as nuloginf
        from NuLogParser import using_GPU

        if using_GPU:
            logging.info("inferencing with GPU.")
        else:
            logging.info("inferencing without GPU.")
        try:
            self.parser = nuloginf.init_model()
            self.nuloginf = nuloginf
            logging.info("Nulog model gets loaded.")
            self.to_reload_model = False
        except Exception as e:
            logging.error("No Nulog model currently {}".format(e))
        # self.predict(test_texts)

    def predict(self, Xs, feature_names=None):
        if self.to_reload_model:
            self.load()
        start_time = time.time()
        # print(x)
        log = Xs["log"]
        output = self.nuloginf.predict(self.parser, log)
        logging.info(
            (
                "--- predict %s logs in %s seconds ---"
                % (len(log), time.time() - start_time)
            )
        )
        return output

    def health_status(self):
        return "still alive"
