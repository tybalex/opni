# Standard Library
import logging
import time
from typing import List

# Third Party
from drain3 import TemplateMiner


def space_tokenize(log: str) -> List[str]:
    """
    a naive white space tokenizer
    """
    return log.split(" ")


class DrainModel:
    def __init__(self, model_uri: str = None, method: str = "predict"):
        self.method = method
        # self.load()
        self.ver = 0.1
        self.load()

    def load(self):
        ## logic to load model
        persistence_type = None
        self.tm = TemplateMiner(persistence_type)
        # self.predict(test_texts)

    def predict(self, Xs, feature_names=None):
        start_time = time.time()
        # output = self.model.predict(x)
        log = Xs["log"]
        output = []
        for x in log:
            tokens = space_tokenize(x)
            msg = " ".join(tokens)
            res = self.tm.add_log_message(msg)
            output.append(res["change_type"])

        output = [0 if o == "none" else 1 for o in output]
        logging.info(
            (
                "--- predict %s logs in %s seconds ---"
                % (len(output), time.time() - start_time)
            )
        )
        return {"drain": output}

    def health_status(self):
        return "still alive"

    def tags(self):
        return {"drain_ver": self.ver}
