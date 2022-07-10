from random import randint
import logging
import time
import zmq
import json
from utils.RepeatTimer import RepeatTimer
import os
from collections import namedtuple
from prettytable import PrettyTable

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

# setup two different type of sockets  to manage data from client and acknoledgment of bein reachable
context = zmq.Context()
responder = context.socket(zmq.REP)
responder.bind("tcp://*:5555")
logging.info("Waiting for client")

subscriber = context.socket(zmq.SUB)
subscriber.bind("tcp://*:5556")
subscriber.connect("tcp://127.0.0.1:5556")

publisher = context.socket(zmq.PUB)
publisher.bind("tcp://*:5557")
publisher.connect("tcp://127.0.0.1:5557")

# subscribe to route 1 updates
subscriber.setsockopt(zmq.SUBSCRIBE, b"1")

# Configure an additional socket to keep track of the sockets registered in the line above
poller = zmq.Poller()
poller.register(responder, zmq.POLLIN)
poller.register(subscriber, zmq.POLLIN)

dataFromClient = {}
stationsToSendInfo = {}

table = PrettyTable()
table.field_names = ["Route", "DataSent", "Station"]

def displayData(dataFromClient):
    #os.system('clear')
    table.clear_rows()
    for key, value in dataFromClient.items():
        number = len(value)
        if not number == 0:
            table.add_row([key, number, value[0].name])
        print(table)

# execute function {displayData} every second
timer = RepeatTimer(1, displayData, (dataFromClient,))  
timer.start()

# parse json object sent from the client
def customRouteDecoder(routeDic):
    return namedtuple('X', routeDic.keys())(*routeDic.values())

# Process messages from both sockets
while True:
    try:
        socks = dict(poller.poll())
    except KeyboardInterrupt:
        break

    if responder in socks:
        route_id, bus_id = responder.recv_string().split("/")
        # simulate latency
        time.sleep(2)
        if not route_id in stationsToSendInfo:
            stationsToSendInfo[route_id] = set([bus_id])
        else:
            existingSet = stationsToSendInfo[route_id]
            existingSet.add(bus_id)
            stationsToSendInfo[route_id] = existingSet
        
        print(stationsToSendInfo)

        responder.send_string(bus_id)

    if subscriber in socks:
        message = subscriber.recv_string()
        data = message.split("/")
        stationInfo = json.loads(data[1], object_hook=customRouteDecoder)
        for route in stationInfo:
            currentRoute = route.dataFromSensor.bus_id
            if currentRoute in dataFromClient:
                pending = dataFromClient.get(currentRoute)
                pending.append(route)
                dataFromClient[currentRoute] = pending
            else:
                dataFromClient[currentRoute] = [route]

            for key, subscribedRoutes in stationsToSendInfo.items():
                el_in_set = route.dataFromSensor.bus_id in subscribedRoutes
                if el_in_set:
                    print(f"sending info to {key}, for route {route.dataFromSensor.bus_id}")
                    #send information to other stations that are listening for the bus id for simplicity only send current time
                    publisher.send_string(f"{key}/{route.dataFromSensor.bus_id}/{int(time.time())}")