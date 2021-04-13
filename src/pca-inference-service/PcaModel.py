# Standard Library
import gc
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
from FeatureExtractor import FeatureExtractor
from PCA import PCA

MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
NATS_SERVER_URL = os.environ["NATS_SERVER_URL"]
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]

DEFAULT_MODELREADY_PAYLOAD = {
    "bucket": "fasttext-pca-models",
    "bucket_files": {
        "model_file": "gensim_model_latest.bin",
        "ngram_file": "gensim_model_latest.bin.wv.vectors_ngrams.npy",
        "ngram_lock_file": "gensim_model_latest.bin.trainables.vectors_ngrams_lockf.npy",
        "pca_model": "pca_model.obj",
        "pca_feature_extractor": "pca_feature_extractor.obj",
    },
}


def space_tokenize(logs: List[str]):
    """
    a naive white space tokenizer
    """
    for log in logs:
        yield log.split(" ")


def div_norm(x):
    """
    return the div norm
    """
    norm_value = np.sqrt(np.sum(x ** 2))  ## L2 norm
    return (x * (1.0 / norm_value)) if norm_value > 0 else x


class PcaModel:
    def __init__(self):
        self.ver = 0.1
        self.is_model_ready = False
        self.model = PCA()
        self.feature_extractor = FeatureExtractor()
        # self.db = pickledb.load('data.db', False) ## TODO: persistant the data.sb in PVC
        self.db = {}
        self.download_from_minio()
        self.load()

    def download_from_minio(
        self,
        decoded_payload: dict = DEFAULT_MODELREADY_PAYLOAD,
    ):

        minio_client = boto3.resource(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
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

    def load(self):
        try:
            self.fasttext_model = gensim.models.FastText.load(self.fasttext_filename)
            self.model = pickle.load(open(self.pca_filename, "rb"))
            self.feature_extractor = pickle.load(
                open(self.feature_extractor_filename, "rb")
            )
            logging.info("model loading success.")
            self.is_model_ready = True
        except Exception as e:
            logging.debug(e)
            logging.error("No FastText model and PCA model is currently available.")

    def get_word_vecs(self, tokens):
        for word in tokens:
            yield self.fasttext_model.wv[word]

    def get_sentence_vectors(self, tokens_list):
        for tokens in tokens_list:
            processed_word_vectors = np.array(
                [div_norm(wv) for wv in self.get_word_vecs(tokens)]
            )
            yield np.mean(processed_word_vectors, axis=0)

    def get_window_vec(self, logs: List[str], window: int):
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

    def predict(self, Xs: dict):
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

    def tags(self):
        return {"pca_ver": self.ver}
