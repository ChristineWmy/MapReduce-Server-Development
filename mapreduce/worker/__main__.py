"""Worker modeule."""

import os
import logging
import json
# import heapq
import time
import subprocess
from pathlib import Path
import socket
import threading
import click


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Worker:
    """Worker class."""

    def __init__(self, tcp_port, udp_port, worker_port):
        """Initialize worker class."""
        logging.info(
            "Starting worker tcp: %s udp: %s worker: %s",
            tcp_port, udp_port, worker_port)
        logging.info("Worker PWD %s", os.getcwd())
        self._signals = {"shutdown": False}
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        self.worker_port = worker_port
        self._hb_thread = threading.Thread(target=self._hb_sender)
        self.pid = os.getpid()
        self._start()
        self._shutdown()

    def tcp_port_(self):
        """Contructor for tcp_port."""
        return self.tcp_port

    def udp_port_(self):
        """Contructor for udp_port."""
        return self.udp_port

    def worker_port_(self):
        """Contructor for worker_port."""
        return self.worker_port

    def _send_registration(self):
        """Send worker registration to manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.connect(("localhost", self.tcp_port))
            msg = json.dumps({
                    "message_type": "register",
                    "worker_host": "localhost",
                    "worker_port": self.worker_port,
                    "worker_pid": self.pid
                })
            sock.sendall(msg.encode('utf-8'))

    def _workport_listener(self):
        """Listen to TCP for incoming job."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.worker_port))
            sock.listen()

            self._send_registration()
            sock.settimeout(1)
            while not self._signals["shutdown"]:
                try:
                    serversocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])
                with serversocket:
                    message_chunks = []
                    while True:
                        try:
                            datas = serversocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not datas:
                            break
                        message_chunks.append(datas)
                # Decode list-of-byte-strings to UTF8 and parse JSON data
                message_byte = b''.join(message_chunks)
                message = message_byte.decode("utf-8")
                try:
                    message = json.loads(message)
                except json.JSONDecodeError:
                    continue
                self._handle_message(message)
                print(message)

    def _hb_sender(self):
        """Send heart beat to manager every 2 seconds."""
        while not self._signals["shutdown"]:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                # Bind the socket to the server
                sock.connect(("localhost", self.udp_port))
                heartbeat = json.dumps({
                        "message_type": "heartbeat",
                        "worker_pid": self.pid
                    })
                sock.sendall(heartbeat.encode('utf-8'))
            time.sleep(2)

    def _send_mapping_status(self, output_files):
        """Send finish message to the manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(("localhost", self.tcp_port))
            heartbeat = json.dumps({
                    "message_type": "status",
                    "output_files": output_files,
                    "status": "finished",
                    "worker_pid": self.pid
                })
            sock.sendall(heartbeat.encode('utf-8'))

    def _send_grouping_status(self, output_file):
        """Send finish message to the manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(("localhost", self.tcp_port))
            heartbeat = json.dumps({
                    "message_type": "status",
                    "output_file": output_file,
                    "status": "finished",
                    "worker_pid": self.pid
                })
            sock.sendall(heartbeat.encode('utf-8'))

    def _handle_shutdown(self):
        """Send out shutdown signal."""
        self._signals["shutdown"] = True

    def _handle_ack(self):
        """Handle ack and start sending heart beat."""
        print(f'worker port={self.worker_port} start')
        self._hb_thread.start()

    def _handle_new_task(self, msg):
        """Handle new task from manager."""
        input_files = msg["input_files"]
        executable = msg["executable"]
        output_directory = msg["output_directory"]
        output_files = []
        logging.info(str(output_directory + input_files[0]))
        # Process each file one at a time
        for input_file in input_files:
            # process = subprocess.Popen(
            # executable, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            with subprocess.Popen(
                executable, stdin=subprocess.PIPE, stdout=subprocess.PIPE) \
                    as process:
                with open(input_file, encoding='utf-8') as file:
                    outs = process.communicate(
                        input=bytes(file.read(), 'utf-8'))[0]
                output_path = Path(output_directory)/Path(input_file).name
                with open(output_path, 'w', encoding='utf-8') as output_file:
                    output_file.write(outs.decode('utf-8'))
                output_files.append(output_path)

            # with open(input_file, encoding='utf-8') as file:
            #     output_path = Path(output_directory + input_file)
        self._send_mapping_status([str(x) for x in output_files])

    def _handle_sort_task(self, msg):
        """Handle new sort task from manager."""
        input_files = msg["input_files"]
        output_file = msg["output_file"]
        input_merge = ""
        for input_file in input_files:
            with open(input_file, encoding='utf-8') as file:
                input_merge += file.read()
        with open(output_file, 'w', encoding='utf-8') as output:
            output.write(input_merge)
        with open(output_file, encoding='utf-8') as file:
            data = file.readlines()
            data.sort()
        os.remove(output_file)
        with open(output_file, 'w', encoding='utf-8') as output:
            out = ""
            output.write(out.join(data))
        self._send_grouping_status(str(output_file))

    # def _handle_sort_task(self, msg):
    #     """Handle new sort task from manager."""
    #     input_files = msg["input_files"]
    #     output_file = msg["output_file"]
    #     input_merge = []
    #     for input_file in input_files:
    #         with open(input_file, encoding='utf-8') as in_file:
    #             lines = in_file.readlines()
    #             lines.sort()
    #             input_merge.append(lines)
    #     out_file = Path(output_file)
    #     if out_file.exists():
    #         output_file.unlink()
    #     out_file.touch()
    #     with open(out_file, 'w', encoding='utf-8') as output:
    #         for line in heapq.merge(*input_merge):
    #             output.write(line)
    #     self._send_grouping_status(str(output_file))

    def _handle_message(self, msg):
        """Handle msg."""
        if msg["message_type"] == "shutdown":
            self._handle_shutdown()
        elif msg["message_type"] == "register_ack":
            self._handle_ack()
        elif msg["message_type"] == "new_worker_task":
            self._handle_new_task(msg)
        elif msg["message_type"] == "new_sort_task":
            self._handle_sort_task(msg)

    def _start(self):
        """Start the worker, ready for new job."""
        self._workport_listener()

    def _shutdown(self):
        """Join threads and shutdown worker."""
        if self._hb_thread.is_alive():
            self._hb_thread.join()
        print(f'worker port={self.worker_port} shutdown')


@click.command()
@click.argument("manager_port", nargs=1, type=int)
@click.argument("manager_hb_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(manager_port, manager_hb_port, worker_port):
    """Start worker."""
    Worker(manager_port, manager_hb_port, worker_port)


if __name__ == '__main__':
    main()
