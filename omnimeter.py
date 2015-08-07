"""
Listens for activity on OM/# topic and for each message spawns a thread to send de data to librato
"""
__author__ = 'Cesar'


import mosquitto
import threading
import logging
import librato


def upload_to_librato(msg):
    """
    :param msg: mqtt topic: OM/F/[id]/[data1, data2 ..]
    :return: N/A (thread)
    """
    # Get elements from mqtt:
    data = msg.payload.split(',')
    topic = msg.topic.split('/')
    device_id = topic[2]
    #   [0] omnimeter.total_kwh,
    #   [1] omnimeter.t1_kwh,
    #   [2] omnimeter.t2_kwh,
    #   [3] omnimeter.t3_kwh,
    #   [4] omnimeter.t4_kwh,
    #   [5]omnimeter.total_rev_kwh,
    #   [6] omnimeter.t1_rev_kwh,
    #   [7] omnimeter.t2_rev_kwh,
    #   [8] omnimeter.t3_rev_kwh,
    #   [9] omnimeter.t4_rev_kwh,
    #   [10] omnimeter.v1,
    #   [11] omnimeter.v2,
    #   [12] omnimeter.v3,
    #   [13] omnimeter.a1,
    #   [14] omnimeter.a2,
    #   [15] omnimeter.a3,
    #   [16] omnimeter.p1,
    #   [17] omnimeter.p2,
    #   [18] omnimeter.p3,
    #   [19] omnimeter.p_total,
    #   [20] omnimeter.cos1,
    #   [21] omnimeter.cos2,
    #   [22] omnimeter.cos3,
    #   [23] omnimeter.max_demand,
    #   [24] omnimeter.date_time)
    try:
        q = librato_api.new_queue()
        q.add('Total_kWh', float(int(data[0])/10.0), source=device_id)
        q.add('Total_Rev_kWh', float(int(data[5])/10.0), source=device_id)
        q.add('Voltage_L1', float(int(data[10])/10.0), source=device_id)
        q.add('Voltage_L2', float(int(data[11])/10.0), source=device_id)
        q.add('Voltage_L3', float(int(data[12])/10.0), source=device_id)
        q.add('Amperage_L1', float(int(data[13])/10.0), source=device_id)
        q.add('Amperage_L2', float(int(data[14])/10.0), source=device_id)
        q.add('Amperage_L3', float(int(data[15])/10.0), source=device_id)
        q.add('Total_Power', float(int(data[19])/10.0), source=device_id)
        q.add('Cos_L1', float(int(data[20])/100.0), source=device_id)
        q.add('Cos_L2', float(int(data[21])/100.0), source=device_id)
        try:
            q.add('Cos_L3', float(int(data[22])/100.0))
        except ValueError:
            # for 'C000' in omnimeter.cos3
            q.add('Cos_L3', float(int(data[22][1:])/100.0))
        q.submit()
    except Exception as e:
        logging.warning('Failed to open connection to Librato! Error = %s', e.__str__())


# Define event callbacks
def on_connect(mosq, obj, rc):
    logging.info("Connected, rc: " + str(rc))
    # Subscribe to Command
    mqttc.subscribe('OM/F/#', 0)


def on_message(mosq, obj, msg):
    logging.debug(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
    t = threading.Thread(target=upload_to_librato, args=[msg])
    t.daemon = True
    t.start()


def on_publish(mosq, obj, mid):
    logging.debug("mid: " + str(mid))


def on_subscribe(mosq, obj, mid, granted_qos):
    logging.info("Subscribed: " + str(mid) + " " + str(granted_qos))


def on_log(mosq, obj, level, string):
    logging.debug(string)


def apiClientDaemon():
    mqttc.loop_forever()

# Connect to Librato
librato_api = librato.connect(username='cesar@petrolog.us',
                              api_key='80d56cc6ce41b3234fa6d900660161ccf6f8e523519d3d636df839f3e4b44830')

# Create Mosquitto Client object
mqttc = mosquitto.Mosquitto("Omnimeter_Liberato_test")

# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe

# Connect
mqttc.connect('54.85.197.66', 1883)
