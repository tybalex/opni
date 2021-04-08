# Standard Library
import asyncio
import logging
import os
import signal
import time

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


async def infer_logs(logs_queue):
    """
    coroutine to get payload from logs_queue, call inference rest API and put predictions to elasticsearch.
    """

    SELDON_ENDPOINT = os.environ[
        "AMBASSADOR_ENDPOINT"
    ]  ## TODO: this is external endpoint at this point, need to change it to internal endpoint
    ES_ENDPOINT = os.environ["ES_ENDPOINT"]

    ## if gateway is seldon, use the hostname of the seldon deployment, else if gateway is ambassador, use ambassador's hostname
    sc = SeldonClient(
        deployment_name="aiops",
        namespace="seldon",
        gateway_endpoint=SELDON_ENDPOINT,
        gateway="seldon",
    )
    logging.info("seldon client endpoint : " + SELDON_ENDPOINT)

    es = AsyncElasticsearch(
        [ES_ENDPOINT], port=9200, http_compress=True  ## is `port=9200` needed?
    )

    async def doc_generator(df):
        for index, document in df.iterrows():
            doc_dict = document.to_dict()
            yield doc_dict

    # while True:

    #     payload = await logs_queue.get()
    #     if payload is None:
    #      continue

    #     start_time = time.time()
    #     df = pd.read_json(payload)
    #     masked_log = list(df["log"]) ## convert to list for sc predict
    #     window_dt = int(df["window_dt"][0])
    #     input_data = {"window": window_dt ,"log":masked_log}

    #     sc_response = sc.predict(json_data=input_data, transport="rest") ## expected to return a dict

    #     predictions = sc_response.response['jsonData']
    while True:

        payload = await logs_queue.get()
        if payload is None:
            continue

        start_time = time.time()
        logging.info("start time {}".format(start_time))
        df = pd.read_json(payload, dtype={"_id": object})
        # df = pd.read_json(payload)
        # masked_log = list(df["masked_log"]) ## convert to list for sc predict
        masked_log = list(df["masked_log"])
        try:
            window_dt = int(
                list(df["window_dt"])[0]
            )  ## this assumes all the window_dt has the same values.
        except Exception as e:
            logging.error(e)
            logging.info("window : ")
            logging.info(df["window_dt"])
            continue
        input_data = {"window": window_dt, "log": masked_log}

        retry = 3
        OK = False
        sleeptime = 0.5

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

            if OK and "combined_pred" not in sc_response.response["jsonData"]:
                OK = False
                retry -= 1

        if not OK:  ## pass this payload if all the retry failed.
            logging.error("failed to make prediction for this payload.")
            continue

        predictions = sc_response.response["jsonData"]

        anomaly_level_map = {
            0: "Normal",
            1: "Suspicious",
            2: "Anomaly",
        }
        try:
            anomaly_level = [
                anomaly_level_map[int(pred)] for pred in predictions["combined_pred"]
            ]
        except:
            print(predictions)
            print(predictions["combined_pred"])

        df["combined_pred"] = predictions["combined_pred"]
        df["anomaly_level"] = anomaly_level

        # filter out df to only include abnormal predictions
        df = df[df["combined_pred"] > 0]

        if len(df) == 0:
            logging.info(
                "No anomalies in this payload of {} logs".format(len(masked_log))
            )
            continue

        df["_op_type"] = "update"
        df["_index"] = "logs"
        df["script"] = ""
        df["script"] = (
            df["script"]
            + "ctx._source.anomaly_predicted_count = "
            + df["combined_pred"].map(str)
            + ";"
            + 'ctx._source.anomaly_level = "'
            + df["anomaly_level"].map(str)
            + '";'
        )
        ## would astype(str) be faster?

        df.rename(columns={"log_id": "_id"}, inplace=True)

        ## TODO: add these extra metadata in correct format
        # if filter_by_column == "nulog_prediction":
        #     df['script'] = df['script'] + "ctx._source.nulog_confidence = " + df['nulog_confidence'].map(str) + ";"
        # elif filter_by_column == "drain_prediction":
        #     df['script'] = df['script'] + "ctx._source.drain_matched_template_id = " + df['matched_template_id'].map(
        #         str) + ";" + " ctx._source.drain_matched_template_support = " + df['matched_template_support'].map(str) + ";"

        try:
            async for ok, result in async_streaming_bulk(
                es, doc_generator(df[["_id", "_op_type", "_index", "script"]])
            ):
                action, result = result.popitem()
                if not ok:
                    logging.error("failed to %s document %s" % ())
        except Exception as e:
            logging.error(e)
        finally:
            logging.info(
                "Updated {} anomalies from {} logs to ES".format(
                    len(df), len(masked_log)
                )
            )
            logging.info("Updated in {} seconds".format(time.time() - start_time))


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
