# Standard Library
import logging
import time


class Combiner:

    # def aggregate(self, Xs, features_names=None):
    #     start_time = time.time()

    #     drain_pred = list(Xs[1]["drain"])
    #     pca_pred = list(Xs[0]["pca"])
    #     logging.info(len(drain_pred))
    #     logging.info(len(pca_pred))
    #     if len(pca_pred) == 1:
    #     	pca_pred = pca_pred * len(drain_pred)
    #     combined_pred = [x + y for x, y in zip(drain_pred, pca_pred)]
    #     res = {"drain" : drain_pred, "pca" : pca_pred, "combined_pred": combined_pred}
    #     if len(Xs) > 2: ## so it has nolog predictions
    #          nulog_pred = list(Xs[2])
    #          res["nulog"] = nulog_pred
    #     logging.info(("--- combined %s logs in %s seconds ---" % (len(combined_pred) ,time.time() - start_time)))
    #     return res

    def aggregate(self, Xs, features_names=None):
        start_time = time.time()

        drain_pred = list(Xs[1]["drain"])
        nulog_pred = list(Xs[0])
        threshold = 0.8
        logging.info(nulog_pred)
        nulog_pred = [1 if p < threshold else 0 for p in nulog_pred]

        logging.info(len(drain_pred))
        logging.info(len(nulog_pred))

        combined_pred = [x + y for x, y in zip(drain_pred, nulog_pred)]
        res = {
            "drain": drain_pred,
            "nulog_pred": nulog_pred,
            "combined_pred": combined_pred,
        }

        logging.info(
            (
                "--- combined %s logs in %s seconds ---"
                % (len(combined_pred), time.time() - start_time)
            )
        )
        return res
