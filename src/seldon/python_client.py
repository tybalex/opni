# Standard Library
import os

# Third Party
from seldon_core.seldon_client import SeldonClient

endpoint = os.getenv("AMBASSADOR_ENDPOINT")
endpoint = "localhost:9000"
sc = SeldonClient(gateway="seldon")


logs = []
for i in range(5):
    logs.append("log_test" + str(i))
    # d = {"window": i // 5 ,"log":["log_test" + str(i)]}

d = {"window": 0, "log": logs}
r = sc.predict(gateway_endpoint=endpoint, json_data=d, transport="rest")
# time.sleep(0.5)

# for i in range(5):
#     d = {"window": 0 ,"log":["log_test" + str(i)]}

#     r = sc.predict(gateway_endpoint=endpoint,json_data=d , transport="rest")
#     time.sleep(0.5)

print(r)
