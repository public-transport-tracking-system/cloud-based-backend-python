from random import randint
import itertools
import logging
import time
import zmq
import json
from utils.RepeatTimer import RepeatTimer
import os
from collections import namedtuple

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

context = zmq.Context()
responder = context.socket(zmq.REP)
responder.bind("tcp://*:5555")
logging.info("Waiting for client")
subscriber = context.socket(zmq.SUB)
subscriber.bind("tcp://*:5556")
subscriber.connect("tcp://127.0.0.1:5556")

subscriber.setsockopt(zmq.SUBSCRIBE, b"1")

poller = zmq.Poller()
poller.register(responder, zmq.POLLIN)
poller.register(subscriber, zmq.POLLIN)

dataFromClient = {}

def displayData(dataFromClient):
    os.system('clear')
    for k, v in dataFromClient.items():
        number = len(v)
        print ("{:<8} {:<15}".format(k, number))

timer = RepeatTimer(1,displayData, (dataFromClient,))  
timer.start()

def customRouteDecoder(routeDic):
    return namedtuple('X', routeDic.keys())(*routeDic.values())

# Process messages from both sockets
while True:
    try:
        socks = dict(poller.poll())
    except KeyboardInterrupt:
        break

    if responder in socks:
        message = responder.recv()
        # simulate latency
        time.sleep(2)
        responder.send(message)

    if subscriber in socks:
        message = subscriber.recv_string()
        data = message.split("/")
        routeInfo = json.loads(data[1], object_hook=customRouteDecoder)
        for route in routeInfo:
            currentRoute = route.dataFromSensor.bus_id
            if currentRoute in dataFromClient:
                pending = dataFromClient.get(currentRoute)
                pending.append(route)
                dataFromClient[currentRoute] = pending
            else:
                dataFromClient[currentRoute] = [route]