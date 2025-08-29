#!/usr/bin/env python3

import socket
import threading
import csv
import json
import argparse
import sys
import time
import datetime

class ThreadedServer(object):
    def __init__(self, host, opt):
        self.host = host
        self.port = opt.port
        self.opt = opt
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.lock = threading.Lock()

    def listen(self):
        """ Listens for incoming client connections """
        self.sock.listen(5)
        print(f"Server listening on {self.host}:{self.port}...")
        
        while True:
            client, address = self.sock.accept()
            client.settimeout(500)
            print(f"New client connected: {address}")
            
            # Start a thread to send market data to the client
            threading.Thread(target=self.sendStreamToClient, args=(client, self.sendCSVfile())).start()

    def sendStreamToClient(self, client, buffer):
        """ Sends grouped market data (all stocks for a given timestamp) to the client """
        for timestamp, data in buffer.items():
            print(f"Sending data for {timestamp}")

            try:
                message = json.dumps({"timestamp": timestamp, "data": data}) + '\n'
                client.sendall(message.encode('utf-8'))
                
                # Sleep to simulate real-time streaming
                time.sleep(self.opt.interval)
            
            except Exception as e:
                print(f"Client disconnected. Error: {e}")
                client.close()
                return False
        
        print("End of data stream")
        client.close()
        return False

    def sendCSVfile(self):
        """ Reads CSV files, groups data by timestamp, and returns a sorted dictionary """
        data_by_timestamp = {}

        for f in self.opt.files:
            print(f'Reading file {f}...')
            with open(f, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                next(reader)  # Skip the header
                
                for row in reader:
                    timestamp = row["timestamp"]
                    
                    # Group all stocks under the same timestamp
                    if timestamp not in data_by_timestamp:
                        data_by_timestamp[timestamp] = []
                    
                    data_by_timestamp[timestamp].append(row)

        return data_by_timestamp

if __name__ == "__main__":
    parser = argparse.ArgumentParser(usage='usage: tcp_server -p port [-f -m]')
    parser.add_argument('-f', '--files', nargs='+', required=True, help="List of CSV files to read from")
    parser.add_argument("-p", "--port", action="store", dest="port", type=int, required=True, help="Port to listen on")
    parser.add_argument("-t", "--time-interval", action="store", dest="interval", type=float, default=1.0, help="Time interval between updates (seconds)")
    
    opt = parser.parse_args()
    ThreadedServer('127.0.0.1', opt).listen()