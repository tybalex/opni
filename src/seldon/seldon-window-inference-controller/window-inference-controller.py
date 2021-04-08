# Standard Library
import asyncio
import gc
import logging
import os
import signal
import time
from collections import defaultdict

# Third Party
import pandas as pd
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_streaming_bulk
from nats.aio.client import Client as NATS
from seldon_core.seldon_client import SeldonClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


async def consume_logs(loop, logs_queue):
    """
    coroutine to consume logs from NATS and put messages to the logs_queue
    """
    nc = NATS()
    NATS_SERVER_URL = os.environ["NATS_SERVER_URL"]

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
        logging.error(e)

    logging.info(f"Connected to NATS at {nc.connected_url.netloc}...")

    def signal_handler():
        if nc.is_closed:
            return
        logging.warning("Disconnecting...")
        loop.create_task(nc.close())

    for sig in ("SIGINT", "SIGTERM"):
        loop.add_signal_handler(getattr(signal, sig), signal_handler)

    async def subscribe_handler(msg):
        subject = msg.subject
        reply = msg.reply
        payload_data = msg.data.decode()
        await logs_queue.put(payload_data)

    await nc.subscribe("preprocessed_logs", "", subscribe_handler)


async def get_seldon_prediction(input_data: dict, sc: SeldonClient):
    retry = 3
    OK = False
    sleeptime = 0.1
    while not OK and retry >= 0:
        try:
            sc_response = sc.predict(
                json_data=input_data, transport="rest"
            )  ## expected to return a dict
            OK = True
        except Exception as e:
            retry -= 1
            if retry < 0:
                break
            else:
                logging.error(e)
                logging.warning(
                    "can't connect to seldon server, retry in {} seconds".format(
                        sleeptime
                    )
                )
                await asyncio.sleep(sleeptime)

        if OK and "pca" not in sc_response.response["jsonData"]:
            OK = False
            retry -= 1
    if OK:
        return sc_response
    else:
        return None


async def doc_generator(df):
    for index, document in df.iterrows():
        doc_dict = document.to_dict()
        yield doc_dict


async def infer_logs(logs_queue):
    """
    coroutine to get payload from logs_queue, call inference rest API and put predictions to elasticsearch.
    """

    SELDON_ENDPOINT = os.environ[
        "AMBASSADOR_ENDPOINT"
    ]  ## TODO: this is external endpoint at this point, need to change it to internal endpoint
    ES_ENDPOINT = os.environ["ES_ENDPOINT"]

    # sc = SeldonClient(deployment_name="aiops", namespace="seldon", gateway_endpoint=SELDON_ENDPOINT, gateway="seldon")
    # logging.info("seldon client endpoint : " + SELDON_ENDPOINT)
    # Third Party
    from PcaModel import PcaModel

    sc = PcaModel()

    es = AsyncElasticsearch([ES_ENDPOINT], port=9200, http_compress=True)

    last_window = -1
    last_payload_df = {}
    ids_dict = defaultdict(list)
    history_window_preds = defaultdict(int)
    logging.info("start fetching payloads.")
    script = 'ctx._source.anomaly_level = ctx._source.anomaly_predicted_count == 0 ? "Normal" : ctx._source.anomaly_predicted_count == 1 ? "Suspicious" : "Anomaly";'

    while True:
        payload = await logs_queue.get()
        if payload is None:
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

            # sc_response = await get_seldon_prediction(input_data, sc)
            # if sc_response is None:
            #     continue
            predictions = sc.predict(input_data)
            # predictions = sc_response.response['jsonData']

            if (
                predictions["pca"] == 1
            ):  ## update the current dataframe if there's anomaly
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

            if window in history_window_preds:
                logging.info(
                    "history pred : {} ; this pred : {}".format(
                        history_window_preds[window], predictions["pca"]
                    )
                )
                logging.info(
                    "log number this time : {} ; log number stored : {}".format(
                        len(masked_log), len(ids_dict[window])
                    )
                )
            if (
                window in history_window_preds
                and history_window_preds[window] != predictions["pca"]
            ):
                ## only consider cases that history_window_preds[window] != predictions["pca"]
                new_df = pd.DataFrame(data={"_id": ids_dict[window]})
                new_df["_op_type"] = "update"
                new_df["_index"] = "logs"
                ## activity to change prev results
                if (
                    history_window_preds[window] == 0 and predictions["pca"] == 1
                ):  ## change pca anomaly from 0 to 1
                    new_df[
                        "script"
                    ] = "ctx._source.anomaly_predicted_count += 1; ctx._source.pca_anomaly = 1;"
                elif (
                    history_window_preds[window] == 1 and predictions["pca"] == 0
                ):  ## change pca anomaly from 1 to 0
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
            history_window_preds[window] = predictions["pca"]
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

    consumer_coroutine = consume_logs(loop, logs_queue)
    inference_coroutine = infer_logs(logs_queue)

    loop.run_until_complete(asyncio.gather(inference_coroutine, consumer_coroutine))
    try:
        loop.run_forever()
    finally:
        loop.close()


if __name__ == "__main__":
    start_inference_controller()
