# Standard Library
import asyncio
import gc
import logging
import os
import time
from collections import defaultdict

# Third Party
import pandas as pd
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_streaming_bulk
from nats_wrapper import NatsWrapper
from PcaModel import PcaModel

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
ES_ENDPOINT = os.environ["ES_ENDPOINT"]


async def consume_logs(loop, logs_queue):
    """
    coroutine to consume logs from NATS and put messages to the logs_queue
    """
    await nw.connect(loop)
    nw.add_signal_handler(loop)
    await nw.subscribe(nats_subject="preprocessed_logs", payload_queue=logs_queue)
    await nw.subscribe(nats_subject="model_ready", payload_queue=logs_queue)


async def doc_generator(df):
    for index, document in df.iterrows():
        doc_dict = document.to_dict()
        yield doc_dict


async def infer_logs(logs_queue):
    """
    coroutine to get payload from logs_queue, call inference rest API and put preds to elasticsearch.
    """

    es = AsyncElasticsearch(
        [ES_ENDPOINT],
        port=9200,
        http_compress=True,
        http_auth=("admin", "admin"),
        verify_certs=False,
        use_ssl=True,
    )
    predictor = PcaModel()

    last_window = -1
    last_payload_df = {}
    ids_dict = defaultdict(list)
    preds_dict = defaultdict(int)
    logging.info("start fetching payloads.")
    script = 'ctx._source.anomaly_level = ctx._source.anomaly_predicted_count == 0 ? "Normal" : ctx._source.anomaly_predicted_count == 1 ? "Suspicious" : "Anomaly";'

    while True:
        payload = await logs_queue.get()
        if payload is None:
            continue

        decoded_payload = json.loads(payload)
        if (
            "bucket" in decoded_payload
            and decoded_payload["bucket"] == "fasttext-pca-models"
        ):
            logging.info("received signal to reload pca model.")
            self.download_from_minio(decoded_payload)
            self.load()
            continue

        this_df = pd.read_json(payload, dtype={"_id": object})
        window_dt = int(
            list(this_df["window_dt"])[0]
        )  ## this assumes all the window_dt has the same values.
        masked_log = list(this_df["masked_log"])
        logging.debug(
            "received {} payload of logs at window : {}".format(
                len(masked_log), window_dt
            )
        )
        if window_dt == last_window:
            last_payload_df = last_payload_df.append(this_df, ignore_index=True)
        else:
            df = last_payload_df
            last_payload_df = this_df
            last_window = window_dt
            if len(df) == 0:
                continue

            start_time = time.time()
            masked_log = list(df["masked_log"])
            window = int(list(df["window_dt"])[0])
            input_data = {"window": window, "log": masked_log}
            logging.info(
                "inferencing on time window : {} of {} accumulated logs".format(
                    window, len(masked_log)
                )
            )

            preds = predictor.predict(input_data)

            if preds["pca"] == 1:
                ## update the current dataframe if there's anomaly
                df["_op_type"] = "update"
                df["_index"] = "logs"
                df["script"] = (
                    "ctx._source.anomaly_predicted_count += 1; ctx._source.pca_anomaly = 1;"
                    + script
                )
                try:
                    async for ok, result in async_streaming_bulk(
                        es, doc_generator(df[["_id", "_op_type", "_index", "script"]])
                    ):
                        action, result = result.popitem()
                        if not ok:
                            logging.error("failed to %s document %s" % ())
                    logging.info("Put {} logs to ES".format(len(df)))
                except Exception as e:
                    logging.error(e)

            if window in preds_dict:
                logging.info(
                    "history pred : {} ; this pred : {}".format(
                        preds_dict[window], preds["pca"]
                    )
                )
                logging.info(
                    "log number this time : {} ; log number stored : {}".format(
                        len(masked_log), len(ids_dict[window])
                    )
                )

            ## the logic to properly update elasticsearch history preds.
            if window in preds_dict and preds_dict[window] != preds["pca"]:
                ## only consider cases that preds_dict[window] != preds["pca"]
                new_df = pd.DataFrame(data={"_id": ids_dict[window]})
                new_df["_op_type"] = "update"
                new_df["_index"] = "logs"
                ## activity to change prev results
                if preds_dict[window] == 0 and preds["pca"] == 1:
                    ## change pca anomaly from 0 to 1
                    new_df[
                        "script"
                    ] = "ctx._source.anomaly_predicted_count += 1; ctx._source.pca_anomaly = 1;"
                elif preds_dict[window] == 1 and preds["pca"] == 0:
                    ## change pca anomaly from 1 to 0
                    new_df[
                        "script"
                    ] = "ctx._source.anomaly_predicted_count -= 1; ctx._source.pca_anomaly = 0;"
                new_df["script"] = new_df["script"] + script
                assert len(ids_dict[window]) == len(new_df)
                try:
                    async for ok, result in async_streaming_bulk(
                        es,
                        doc_generator(new_df[["_id", "_op_type", "_index", "script"]]),
                    ):
                        action, result = result.popitem()
                        if not ok:
                            logging.error("failed to %s document %s" % ())
                    logging.info("updated {} logs to ES".format(len(new_df)))
                except Exception as e:
                    logging.error(e)

                del new_df

            ids_dict[window].extend(list(df["_id"]))
            preds_dict[window] = preds["pca"]
            logging.info("processed in {} seconds".format(time.time() - start_time))

            del df
            del input_data
            del masked_log

        del payload
        gc.collect()


def start_inference_controller():
    """
    entry of inference controller.
    """
    loop = asyncio.get_event_loop()
    logs_queue = asyncio.Queue(loop=loop)
    nw = NatsWrapper()

    consumer_coroutine = consume_logs(nw, loop, logs_queue)
    inference_coroutine = infer_logs(logs_queue)

    loop.run_until_complete(asyncio.gather(inference_coroutine, consumer_coroutine))
    try:
        loop.run_forever()
    finally:
        loop.close()


if __name__ == "__main__":
    start_inference_controller()
