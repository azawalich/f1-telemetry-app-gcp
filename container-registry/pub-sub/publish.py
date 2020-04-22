import socket
from f1_2019_telemetry.packets import unpack_udp_packet
from google.cloud import pubsub_v1
import jsonpickle

secrets = {}

f = open('secrets.sh', 'r')
lines_read = f.read().splitlines()[1:]
f.close()

for line in lines_read:
    line_splitted = line.replace('\n', '').replace('"', '').split('=')
    secrets[line_splitted[0]] = line_splitted[1]

project_name = secrets['project_name']
topic_name = secrets['topic_name']

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_name, topic_name)

udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
udp_socket.bind(('0.0.0.0', 5005))

f1_game = True

print('Listening for packets... F1: {}'.format(f1_game))

if f1_game == True:
    while True:
        udp_packet = udp_socket.recv(2048)
        packet = unpack_udp_packet(udp_packet)
        packet_encoded = jsonpickle.encode(packet)
        future = publisher.publish(topic_path, data=packet_encoded.encode())
        print(future.result())
else:
    while True:
        udp_packet, addr = udp_socket.recvfrom(1024)
        print("Received:", udp_packet)