# Standard Library
import asyncio
import gc
import json
import logging
import os
import pickle
import time
from typing import List

# Third Party
import boto3
import gensim
import numpy as np
from botocore.config import Config
from nats.aio.client import Client as NATS

# MINIO_ACCESS_KEY = "myaccesskey"
# MINIO_SECRET_KEY = "mysecretkey"
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


## for ref this is how SKLEARN SERVER is implemented:
## https://github.com/SeldonIO/seldon-core/blob/d84b97431c49602d25f6f5397ba540769ec695d9/servers/sklearnserver/sklearnserver/SKLearnServer.py#L16-L23


def space_tokenize(logs: List[str]):
    """
    a naive white space tokenizer
    """
    for log in logs:
        yield log.split(" ")


def div_norm(x):
    norm_value = np.sqrt(np.sum(x ** 2))  ## L2 norm
    return (x * (1.0 / norm_value)) if norm_value > 0 else x


class PcaModel:
    def __init__(self, model_uri: str = None, method: str = "predict"):
        self.method = method
        self.ver = 0.1
        self.is_model_ready = False
        # self.db = pickledb.load('data.db', False) ## TODO: persistant the data.sb in PVC
        self.db = {}
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
                    if decoded_payload["bucket"] == "fasttext-pca-models":
                        logging.info(
                            "Just received signal to download FastText/PCA model files from Minio."
                        )
                        self.download_from_minio(decoded_payload)
                        self.load()
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
            "bucket": "fasttext-pca-models",
            "bucket_files": {
                "model_file": "gensim_model_latest.bin",
                "ngram_file": "gensim_model_latest.bin.wv.vectors_ngrams.npy",
                "ngram_lock_file": "gensim_model_latest.bin.trainables.vectors_ngrams_lockf.npy",
                "pca_model": "pca_model.obj",
                "pca_feature_extractor": "pca_feature_extractor.obj",
            },
        },
    ):

        endpoint_url = "http://minio.default.svc.cluster.local:9000"
        # endpoint_url = "http://localhost:9001"
        minio_client = boto3.resource(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
        )

        bucket_name = decoded_payload["bucket"]
        bucket_files = decoded_payload["bucket_files"]
        self.fasttext_filename = bucket_files["model_file"]
        self.pca_filename = bucket_files["pca_model"]
        self.feature_extractor_filename = bucket_files["pca_feature_extractor"]
        for k in bucket_files:
            try:
                minio_client.meta.client.download_file(
                    bucket_name, bucket_files[k], bucket_files[k]
                )
            except Exception as e:
                logging.error(
                    "Cannot currently obtain necessary model files. Exiting function"
                )
                return
        self.is_model_ready = True

        ## new
        self.load()

    def load(self):
        if self.is_model_ready:
            self.fasttext_model = gensim.models.FastText.load(self.fasttext_filename)
            logging.info("FastText model loaded...")
            self.model = pickle.load(open(self.pca_filename, "rb"))
            logging.info("PCA model loaded...")
            self.feature_extractor = pickle.load(
                open(self.feature_extractor_filename, "rb")
            )
            logging.info("Feature extractor loaded...")
        else:
            logging.info("No FastText model and PCA model is currently available.")

    def get_word_vecs(self, tokens):
        for word in tokens:
            yield self.fasttext_model.wv[word]

    def get_sentence_vectors(self, tokens_list):
        for tokens in tokens_list:
            processed_word_vectors = np.array(
                [div_norm(wv) for wv in self.get_word_vecs(tokens)]
            )
            yield np.mean(processed_word_vectors, axis=0)

    def get_window_vec(self, logs: List[str], window):
        start_time = time.time()
        window = str(window)
        # vecs = [self.get_sentence_vectors(tokens) for tokens in tokenized] ## this is too slow
        vecs = self.get_sentence_vectors(space_tokenize(logs))

        # prev_vecs = self.db.get(window)
        # if prev_vecs is not False:
        #     prev_vecs = np.array(prev_vecs)
        #     vecs.append(prev_vecs)

        window_vec = np.sum(vecs, axis=0)
        if window in self.db:
            logging.info("loading from prev window vecs")
            prev_vec = self.db[window]
            window_vec = np.sum([window_vec, np.array(prev_vec)], axis=0)

        self.db[window] = window_vec.tolist()

        # self.db.set(window, window_vec.tolist() )
        # self.db.dump()
        del vecs
        gc.collect()

        logging.info(("--- sum vector in %s seconds ---" % (time.time() - start_time)))
        return window_vec

    def predict(self, Xs: dict, feature_names=None):
        window = Xs["window"]
        log = Xs["log"]
        if not self.is_model_ready:
            logging.info("FastText/PCA model is not available yet")
            return {"pca": [-1] * len(log)}

        start_time = time.time()
        window_vec = self.get_window_vec(log, window)

        x_test = self.feature_extractor.transform([window_vec])
        pred = self.model.predict(x_test)
        logging.info(
            (
                "--- predict %s logs in %s seconds ---"
                % (len(log), time.time() - start_time)
            )
        )
        assert len(pred) == 1

        del log
        del Xs
        gc.collect()

        return {"pca": int(pred[0])}

    def health_status(self):
        return "still alive"

    def tags(self):
        return {"pca_ver": self.ver}
