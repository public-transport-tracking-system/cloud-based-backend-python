from random import randint
import itertools
import logging
import time
import zmq
import json

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

context = zmq.Context()
responder = context.socket(zmq.REP)
responder.bind("tcp://*:5555")

subscriber = context.socket(zmq.SUB)
subscriber.connect("tcp://localhost:5556")

subscriber.setsockopt(zmq.SUBSCRIBE, b"100")
subscriber.setsockopt(zmq.SUBSCRIBE, b"101")
subscriber.setsockopt(zmq.SUBSCRIBE, b"102")

poller = zmq.Poller()
poller.register(responder, zmq.POLLIN)
poller.register(subscriber, zmq.POLLIN)

# Process messages from both sockets
while True:
    try:
        socks = dict(poller.poll())
    except KeyboardInterrupt:
        break

    if responder in socks:
        message = responder.recv()
        logging.info("Normal request (%s)", message)
        time.sleep(5)
        logging.info("sent request (%s)", message)
        responder.send(message)
        # process task

    if subscriber in socks:
        message = subscriber.recv()
        logging.info("Publish request (%s)", message)
        # process weather update

# def restartServerAfterCrash():
#     responder.setsockopt(zmq.LINGER, 0)
#     responder.close()
#     context = zmq.Context()
#     responder = context.socket(zmq.REP)
#     responder.bind("tcp://*:5555")


# for cycles in itertools.count():
#     request = responder.recv()
#     #Simulate various problems, after a few cycles
#     if cycles > 10 and randint(0, 3) == 0:
#         logging.info("Simulating a crash")
#         time.sleep(10)
#         restartServerAfterCrash()
#     else:
#         logging.info("Normal request (%s)", request)
#         time.sleep(1)  # Do some heavy work
#         responder.send(request)


