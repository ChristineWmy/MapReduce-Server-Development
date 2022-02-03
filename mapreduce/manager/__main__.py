"""Manger modeule."""

import os
import logging
import json
import time
import heapq
import threading
from pathlib import Path
import shutil
import socket
import sys
from contextlib import ExitStack
from queue import Queue

import click
from mapreduce.utils import WorkerState, ManagerState, ManagerStage


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Manager:
    """Manager class."""

    def __init__(self, tcp_port, udp_port):
        """Initialize manager class."""
        logging.info("Starting manager, tcp: %s udp: %s", tcp_port, udp_port)
        logging.info("Manager PWD %s", os.getcwd())

        # self.tmp = Path('tmp')
        # self.tmp.mkdir(exist_ok=True)
        # for directory in self.tmp.glob('job-*'):
        # #     # remove old dir in tmp folder
        #     shutil.rmtree(directory)
        # # port number
        # self.tcp_port = tcp_port
        # self.udp_port = udp_port

        # # shutdown signal
        # self._signals = {"shutdown": False}
        # # READY, BUSY
        # self._state = ManagerState.READY
        # # MAPPING, GROUPING, REDUCING
        # self._stage = ManagerStage.MAPPING

        # Save attributes into one dictionary
        self.info_dict = {
            'tmp': Path('tmp'),
            'tcp_port': tcp_port,
            'udp_port': udp_port,
            '_signals': {"shutdown": False},
            '_state': ManagerState.READY,
            '_stage': ManagerStage.MAPPING,
            # keep track of job id
            '_job_counter': 0
        }
        for directory in self.info_dict['tmp'].glob('job-*'):
            # remove old dir in tmp folder
            shutil.rmtree(directory)

        # # worker_pid: {worker_port, worker_host, worker_state}
        # self._workers = {}
        # # data structure for workers and assigned job
        # self._worker_job = {}
        # # keep track of the missing heartbeat numbers
        # self._worker_heartbeat = {}

        # # keep track of job id
        # self._job_counter = 0

        # # (job_id, job_msg)
        # self._job_queue = Queue()
        # # keep tract for task
        # self._task_queue = Queue()

        self._task_job_queue = {
            '_job_queue': Queue(),  # (job_id, job_msg)
            '_task_queue': Queue()
        }
        # keep tract of current job (job_id, job_msg)
        self._current_job = ()

        self.worker_info = {
            '_workers': {},
            '_worker_job': {},
            '_worker_heartbeat': {}
        }

        # keep track of numbers of grouping completed
        # self._num_groups = 0
        # self._num_sorting_finished = 0
        # self._grouping_has_finished = True
        self._grouper_finished = {
            '_num_groups': 0,
            '_num_sorting_finished': 0
        }

        # # keep track of processed file for mapper
        # self._processed_file_mapper = {}

        # # Keep track of processed file for reducer
        # self._processed_file_reducer = {}

        self._finished_files = {
            '_processed_file_mapper': {},
            '_processed_file_reducer': {}
        }

        # create thread
        self._udp_thread = threading.Thread(
            target=self._heartbeat_listener)
        # run the server
        self._start()
        # shutdown
        self._shutdown()

    def tcp_port(self):
        """Contructor for tcp_port."""
        return self.info_dict['tcp_port']

    def udp_port(self):
        """Contructor for udp_port."""
        return self.info_dict['udp_port']

    # Receiving stuff for port=======================================
    def _heartbeat_listener(self):
        """Listen to UDP/Heart Beat."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Bind the UDP socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.info_dict['udp_port']))
            sock.settimeout(1)
            # No sock.listen() for UDP
            # Receive incoming UDP messages
            while not self.info_dict['_signals']["shutdown"]:
                for worker, worker_time in \
                        self.worker_info['_worker_heartbeat'].items():
                    if (time.time() - worker_time) > 10 and \
                        self.worker_info['_workers'][worker]["worker_state"]\
                            != WorkerState.DEAD:
                        self.worker_info['_workers'][worker]["worker_state"]\
                            = WorkerState.DEAD
                        if self.worker_info['_worker_job'][worker] != "":
                            self._task_job_queue['_task_queue'].put(
                                self.worker_info['_worker_job'][worker])
                        self.worker_info['_worker_job'][worker] = ""
                        ready_workers = self._get_ready_workers()
                        if ready_workers:
                            if self.info_dict['_stage'] == \
                               ManagerStage.MAPPING:
                                self._process_mapping_status(ready_workers[0])
                            elif self.info_dict['_stage'] == \
                                    ManagerStage.GROUPING:
                                self._process_grouping_status(ready_workers[0])
                            elif self.info_dict['_stage'] == \
                                    ManagerStage.REDUCING:
                                self._process_reducing_status(ready_workers[0])
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                msg = message_bytes.decode("utf-8")
                try:
                    msg = json.loads(msg)
                except json.JSONDecodeError:
                    continue
                self.worker_info['_worker_heartbeat'][msg["worker_pid"]] = \
                    time.time()
                # print(msg)
        print("shutdown heartbeat listener")

    def _tcp_listener(self):
        """Listen to TCP."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.info_dict['tcp_port']))
            sock.listen()
            sock.settimeout(1)
            while not self.info_dict['_signals']["shutdown"]:
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])
                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)
                # Decode list-of-byte-strings to UTF8 and parse JSON data
                message_bytes = b''.join(message_chunks)
                msg = message_bytes.decode("utf-8")
                try:
                    msg = json.loads(msg)
                except json.JSONDecodeError:
                    continue
                # print(msg)
                self._handle_message(msg)
        print("shutdown tcp listener")

    # sending stuff===========================================================
    @classmethod
    def _send_ack(cls, msg):
        """Send ack back to workers."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.connect((msg["worker_host"], msg["worker_port"]))
            ack = json.dumps({
                    "message_type": "register_ack",
                    "worker_host": msg["worker_host"],
                    "worker_port": msg["worker_port"],
                    "worker_pid": msg["worker_pid"]
                })
            sock.sendall(ack.encode('utf-8'))

    # handling stuff===========================================================
    def _handle_shutdown(self):
        """Send out shudown signal."""
        self.info_dict['_signals']["shutdown"] = True

    def _handle_register(self, msg):
        """Regsiter workers."""
        worker_pid = msg["worker_pid"]
        worker_info = {
            "worker_host": msg["worker_host"],
            "worker_port": msg["worker_port"],
            "worker_state": WorkerState.READY  # register worker as ready
        }
        self.worker_info['_workers'][worker_pid] = worker_info
        # Keep track of heartbeat start time
        self.worker_info['_worker_heartbeat'][worker_pid] = time.time()
        self._send_ack(msg)

        # Check whether the task queue is empty
        if not self._task_job_queue['_task_queue'].empty():
            # (job_id, job_msg)
            current_job_id, current_msg = self._current_job
            ready_workers = self._get_ready_workers()
            for worker_pid in ready_workers:
                # give out as many job as possible to ready workers
                input_files = self._task_job_queue['_task_queue'].get()
                if self.info_dict['_stage'] == ManagerStage.MAPPING:
                    # output_dir_mapper = \
                    #     self.info_dict['tmp']/("job-" +
                    #                            str(current_job_id))/"mapper-output"
                    output_dir_mapper = Path(str(self.info_dict['tmp']) +
                                             '/job-' + str(current_job_id) +
                                             '/mapper-output')
                    self._send_mapping_job(
                        input_files,
                        current_msg["mapper_executable"],
                        output_dir_mapper,
                        worker_pid)
                elif self.info_dict['_stage'] == ManagerStage.REDUCING:
                    # output_dir_reducer = \
                    #     self.info_dict['tmp']/("job-" +
                    #                            str(current_job_id))/"reducer-output"
                    output_dir_reducer = Path(str(self.info_dict['tmp']) +
                                              '/job-' + str(current_job_id) +
                                              '/reducer-output')
                    self._send_reducing_job(
                        input_files,
                        current_msg["reducer_executable"],
                        output_dir_reducer,
                        worker_pid)
                # change worker status
                self.worker_info['_workers'][worker_pid]["worker_state"] = \
                    WorkerState.BUSY

    def _handle_newjob(self, msg):
        """Handle new incoming job."""
        # create new dir
        self._create_dir()  # create directory for job = job_id
        # add job to queue
        available_workers = self._get_ready_workers()
        if (self.info_dict['_state'] == ManagerState.BUSY) or \
                (len(available_workers) == 0):
            self._task_job_queue['_job_queue'].\
                put((self.info_dict['_job_counter'], msg))
            self.info_dict['_job_counter'] += 1
        else:
            self._start_job(self.info_dict['_job_counter'], msg)
            self.info_dict['_job_counter'] += 1

    def _handle_status(self, msg):
        """Handle incoming status message."""
        worker_pid = msg["worker_pid"]
        # print(self._finished_files['_processed_file_mapper'])
        # clear the worker job
        self.worker_info['_worker_job'][worker_pid] = ""
        if self.info_dict['_stage'] == ManagerStage.MAPPING:
            # keep track of which file has been processed
            output_files = msg["output_files"]
            for files in output_files:
                filename = Path(files).name
                self._finished_files['_processed_file_mapper'][filename] = True
            # give worker new job depending on task queue
            self._process_mapping_status(worker_pid)
            # check for termiantion of mapping stage
            if self._mapping_finished():
                logging.info(
                    "Manager:%s end map stage",
                    self.info_dict['tcp_port'])
                self._execute_sorting()
        elif self.info_dict['_stage'] == ManagerStage.GROUPING:
            self.worker_info['_workers'][worker_pid]["worker_state"] = \
                WorkerState.READY
            # self._process_grouping_status(worker_pid)
            self._grouper_finished['_num_sorting_finished'] += 1
            # If sorting has finished
            if self._grouper_finished['_num_groups'] == \
                    self._grouper_finished['_num_sorting_finished']:
                # Reset the flag of finished sorting groups
                self._grouper_finished['_num_sorting_finished'] = 0
                current_job_id, current_job_msg = self._current_job
                # output_dir = \
                #     self.info_dict['tmp']/("job-" +
                #                            str(current_job_id))/"grouper-output"
                output_dir = Path(str(self.info_dict['tmp']) +
                                  '/job-' + str(current_job_id) +
                                  '/grouper-output')
                num_reducers = current_job_msg["num_reducers"]
                self._group_to_reduce(output_dir, num_reducers)
                logging.info(
                    "Manager:%s end group stage",
                    self.info_dict['tcp_port'])
                self._execute_reducing()
        elif self.info_dict['_stage'] == ManagerStage.REDUCING:
            # keep track of which file has been processed
            output_files = msg["output_files"]
            for files in output_files:
                filename = Path(files).name
                self._finished_files['_processed_file_reducer'][filename] = \
                    True
            # give worker new job depending on task queue
            self._process_reducing_status(worker_pid)
            # check for termiantion of reducing stage
            if self._reducing_finished():
                logging.info(
                    "Manager:%s end reduce stage",
                    self.info_dict['tcp_port'])
                self._move_result_files()
                self.info_dict['_state'] = ManagerState.READY
                self.worker_info['_workers'][worker_pid]["worker_state"] = \
                    WorkerState.READY
                # if self._task_job_queue['_job_queue'].empty():
                #     self._tcp_listener()
                if not self._task_job_queue['_job_queue'].empty():
                    self._current_job = \
                        self._task_job_queue['_job_queue'].get()
                    current_job_id, current_job_msg = \
                        self._current_job
                    self._start_job(current_job_id, current_job_msg)
        else:
            # exit(1)
            sys.exit(1)

    def _handle_message(self, msg):
        """Handle msg."""
        if msg["message_type"] == "shutdown":
            self._handle_shutdown()
        elif msg["message_type"] == "register":
            self._handle_register(msg)
        elif msg["message_type"] == "new_manager_job":
            self._handle_newjob(msg)
        elif msg["message_type"] == "status":
            self._handle_status(msg)

    # helper util function=====================================================
    def _create_dir(self):
        """Create directorys for new job."""
        # newjob_dir = self.info_dict['tmp']/("job-" +
        #                                     str(self.info_dict['_job_counter']))
        newjob_dir = Path(str(self.info_dict['tmp']) +
                          "/job-" + str(self.info_dict['_job_counter']))
        newjob_dir.mkdir()
        (newjob_dir/"mapper-output/").mkdir()
        (newjob_dir/"grouper-output/").mkdir()
        (newjob_dir/"reducer-output/").mkdir()

    def _get_ready_workers(self):
        """Get workers thats ready to work."""
        ready_workers = []
        for worker_pid, worker_info in self.worker_info['_workers'].items():
            if worker_info['worker_state'] == WorkerState.READY:
                ready_workers.append(worker_pid)
        # for worker_pid in self.worker_info['_workers']:
        #     if self.worker_info['_workers'][worker_pid]["worker_state"] == \
        #             WorkerState.READY:
        #         print(worker_pid)
        #         ready_workers.append(worker_pid)
        return ready_workers

    @classmethod
    def _input_partition(cls, input_dir, num_worker):
        """Partition input directory into queue of list (Map & Reduce only)."""
        partition = [[] for i in range(num_worker)]
        if "grouper-output" in str(input_dir):
            input_files = [str(x) for x in Path(input_dir).glob("reduce*")]
        else:
            input_files = [str(x) for x in Path(input_dir).glob("*")]
        input_files.sort()
        for index, file in enumerate(input_files):
            partition[index % num_worker].append(file)
        queue_partition = Queue()
        for job in partition:
            queue_partition.put(job)
        # keep track of which file has been processed
        if "grouper-output" in str(input_dir):
            processed_file = \
                {x.name: False for x in Path(input_dir).glob("reduce*")}
        else:
            processed_file = \
                {x.name: False for x in Path(input_dir).glob("*")}
        return queue_partition, processed_file

    def _start_job(self, job_id, msg):
        """Execute job described in the msg."""
        self.info_dict['_state'] = ManagerState.BUSY
        self._current_job = (job_id, msg)
        self._execute_mapping(job_id, msg)

    # mapping function=========================================================
    def _mapping_finished(self):
        """Check if mapping stage is finish."""
        for file in self._finished_files['_processed_file_mapper']:
            if not self._finished_files['_processed_file_mapper'][file]:
                return False
        return True

    def _process_mapping_status(self, worker_pid):
        """Process mapping status, send worker new job if need to."""
        # worker with pid = worker_pid is now free
        self.worker_info['_workers'][worker_pid]["worker_state"] = \
            WorkerState.READY
        if self._task_job_queue['_task_queue'].empty():
            # no more task left for mapping, return
            return
        # task queue isnt empty, assign new task to this ready worker
        # print(self._task_job_queue['_task_queue'].qsize())
        input_files = self._task_job_queue['_task_queue'].get()
        # get current job info
        current_job_id, current_job_msg = self._current_job
        mapper_executable = current_job_msg["mapper_executable"]
        output_dir_mapper = \
            self.info_dict['tmp']/("job-" +
                                   str(current_job_id))/"mapper-output"
        # output_dir_mapper = Path(str(self.info_dict['tmp']) +
        #                          '/job-' + str(current_job_id) +
        #                          '/mapper-output')
        # send job to worker
        self._send_mapping_job(
            input_files, mapper_executable, output_dir_mapper, worker_pid)
        # update worker status
        self.worker_info['_workers'][worker_pid]["worker_state"] = \
            WorkerState.BUSY

    def _send_mapping_job(
            self, input_files, executable, output_directory, worker_pid):
        """Send worker new mapping job."""
        worker = self.worker_info['_workers'][worker_pid]
        # keep track of worker job
        self.worker_info['_worker_job'][worker_pid] = input_files
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.connect((worker["worker_host"], worker["worker_port"]))
            ack = json.dumps({
                    "message_type": "new_worker_task",
                    "input_files": input_files,
                    "executable": executable,
                    "output_directory": str(output_directory),
                    "worker_pid": worker_pid
                })
            sock.sendall(ack.encode('utf-8'))

    def _execute_mapping(self, job_id, msg):
        """Start executing mapping stage."""
        logging.info("Manager:%s begin map stage", self.info_dict['tcp_port'])
        self.info_dict['_stage'] = ManagerStage.MAPPING

        input_dir = msg["input_directory"]
        num_mappers = msg["num_mappers"]
        mapper_executable = msg["mapper_executable"]
        output_dir_mapper = \
            self.info_dict['tmp']/("job-" +
                                   str(job_id))/"mapper-output"

        self._task_job_queue['_task_queue'], \
            self._finished_files['_processed_file_mapper'] = \
            self._input_partition(input_dir, num_mappers)
        # print(self._task_job_queue['_task_queue'].qsize())

        ready_workers = self._get_ready_workers()
        for worker_pid in ready_workers:
            # give out as many job as possible to ready workers
            if self._task_job_queue['_task_queue'].empty():
                # no more job from queue left to give
                break
            # send worker job
            input_files = self._task_job_queue['_task_queue'].get()
            self._send_mapping_job(
                input_files, mapper_executable, output_dir_mapper, worker_pid)
            # change worker status
            self.worker_info['_workers'][worker_pid]["worker_state"] = \
                WorkerState.BUSY

    # grouping function========================================================
    # def _grouping_finished(self):
    #     """Check if grouping stage is finish."""
    #     return self._grouping_has_finished

    def _process_grouping_status(self, worker_pid):
        """Process mapping status, send worker new job if need to."""
        # worker with pid = worker_pid is now free
        self.worker_info['_workers'][worker_pid]["worker_state"] = \
            WorkerState.READY
        if self._task_job_queue['_task_queue'].empty():
            # no more task left for grouping, return
            return
        # task queue isnt empty, assign new task to this ready worker
        # print(self._task_job_queue['_task_queue'].qsize())
        input_files = self._task_job_queue['_task_queue'].get()
        # get current job info
        current_job_id = self._current_job[0]
        output_dir_grouper = \
            self.info_dict['tmp']/("job-" +
                                   str(current_job_id))/"grouper-output"
        # send job to worker
        self._send_grouping_job(
            input_files, output_dir_grouper, worker_pid)
        # update worker status
        self.worker_info['_workers'][worker_pid]["worker_state"] = \
            WorkerState.BUSY

    def _send_grouping_job(
                self, input_files, output_file, worker_pid):
        """Send Gourping Files."""
        worker = self.worker_info['_workers'][worker_pid]
        # keep track of worker job
        self.worker_info['_worker_job'][worker_pid] = input_files
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the ssocket to the server
            sock.connect((worker["worker_host"], worker["worker_port"]))
            ack = json.dumps({
                    "message_type": "new_sort_task",
                    "input_files": input_files,
                    "output_file": str(output_file),
                    "worker_pid": worker_pid
                })
            sock.sendall(ack.encode('utf-8'))

    @classmethod
    def _group_to_reduce(cls, input_dir, num_reducers):
        """Group to files which can be reduce."""
        # files = []
        # reduce = [[] for i in range(num_reducers)]
        input_files = [str(x) for x in Path(input_dir).glob("*")]
        # for input_file in input_files:
        #     files.append(open(input_file))
        # Use ExitStack to avoid close files atomatically by with
        with ExitStack() as outer_stack:
            files = [outer_stack.enter_context(
                open(input_file, encoding='utf-8'))
                    for input_file in input_files]
            sort_file = []
            output_directories = []
            for i in range(num_reducers):
                # num = str(i + 1).zfill(2)
                # output_file = f"reduce{num}"
                output_file = f"reduce{str(i + 1).zfill(2)}"
                output_directory = input_dir/str(output_file)
                output_directories.append(str(output_directory))
            # print(output_directories)
            # sort_file.append(open(output_directory, 'w'))
            with ExitStack() as inner_stack:
                sort_file = [inner_stack.enter_context(
                    open(output_file, 'w', encoding='utf-8'))
                        for output_file in output_directories]
                count = -1
                prev = []
                for line in heapq.merge(*files):
                    if line != prev:
                        prev = line
                        count += 1
                    sort_file[count % num_reducers].write(line)

        # for file in files:
        #     file.close()
        # for file in sort_file:
        #     file.close()

        # Write to Reduce File
        # for i in range(num_reducers):
        #     num = str(i + 1).zfill(2)
        #     output_file = "reduce{}".format(num)
        #     output_directory = input_dir/str(output_file)
        #     with open(output_directory, 'w') as file:
        #         out = ""
        #         file.write(out.join(reduce[i]))

    def _execute_sorting(self):
        """Start executing sorting."""
        logging.info(
            "Manager:%s begin group stage",
            self.info_dict['tcp_port'])
        self.info_dict['_stage'] = ManagerStage.GROUPING
        # self._grouping_has_finished = False

        # Step1: Workers sort each mapper output file
        current_job_id = self._current_job[0]
        input_dir = \
            self.info_dict['tmp']/("job-" +
                                   str(current_job_id))/"mapper-output"
        output_dir = \
            self.info_dict['tmp']/("job-" +
                                   str(current_job_id))/"grouper-output"
        # num_reducers = current_job_msg["num_reducers"]
        count = 1

        ready_workers = self._get_ready_workers()
        self._grouper_finished['_num_groups'] = len(ready_workers)
        self._task_job_queue['_task_queue'], \
            self._finished_files['_processed_file_mapper'] = \
            self._input_partition(input_dir,
                                  self._grouper_finished['_num_groups'])

        for worker_pid in ready_workers:
            # give out all possible job to ready workers
            if self._task_job_queue['_task_queue'].empty():
                # no more job from queue left to give
                break
            # send worker job
            input_files = self._task_job_queue['_task_queue'].get()
            num = str(count).zfill(2)
            output_file = f"sorted{num}"
            output_directory = output_dir/str(output_file)
            count += 1
            self._send_grouping_job(
                input_files, output_directory, worker_pid)
            # change worker status
            self.worker_info['_workers'][worker_pid]["worker_state"] = \
                WorkerState.BUSY

        # Step2: Manager rearranges sorted mapper output files
        # self._group_to_reduce(output_dir, num_reducers)
        # self._grouping_has_finished = True

    # reducing function========================================================
    def _reducing_finished(self):
        """Check if reducing stage is finish."""
        for file in self._finished_files['_processed_file_reducer']:
            if not self._finished_files['_processed_file_reducer'][file]:
                return False
        return True

    def _send_reducing_job(
            self, input_files, executable, output_directory, worker_pid):
        """Send worker new reducing job."""
        worker = self.worker_info['_workers'][worker_pid]
        # keep track of worker job
        self.worker_info['_worker_job'][worker_pid] = input_files
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.connect((worker["worker_host"], worker["worker_port"]))
            ack = json.dumps({
                    "message_type": "new_worker_task",
                    "input_files": input_files,
                    "executable": executable,
                    "output_directory": str(output_directory),
                    "worker_pid": worker_pid
                })
            sock.sendall(ack.encode('utf-8'))

    def _execute_reducing(self):
        """Start executing reducing stage."""
        logging.info(
            "Manager:%s begin reduce stage",
            self.info_dict['tcp_port'])
        self.info_dict['_stage'] = ManagerStage.REDUCING
        job_id, msg = self._current_job

        input_dir = \
            self.info_dict['tmp']/("job-" + str(job_id))/"grouper-output"
        num_reducers = msg["num_reducers"]
        reducer_executable = msg["reducer_executable"]
        output_dir_reducer = \
            self.info_dict['tmp']/("job-" + str(job_id))/"reducer-output"

        self._task_job_queue['_task_queue'], \
            self._finished_files['_processed_file_reducer'] = \
            self._input_partition(input_dir, num_reducers)
        ready_workers = self._get_ready_workers()

        for worker_pid in ready_workers:
            if self._task_job_queue['_task_queue'].empty():
                break
            input_files = self._task_job_queue['_task_queue'].get()
            self._send_reducing_job(
                input_files,
                reducer_executable,
                output_dir_reducer,
                worker_pid)
            # Change worker status
            self.worker_info['_workers'][worker_pid]['worker_state'] = \
                WorkerState.BUSY

    def _process_reducing_status(self, worker_pid):
        """Process reducing status, send worker new job if need to."""
        self.worker_info['_workers'][worker_pid]['worker_state'] = \
            WorkerState.READY
        if self._task_job_queue['_task_queue'].empty():
            return
        input_files = self._task_job_queue['_task_queue'].get()
        # Get current job info
        current_job_id, current_job_msg = self._current_job
        reducer_executable = current_job_msg['reducer_executable']
        output_dir_reducer = \
            self.info_dict['tmp']/('job-' +
                                   str(current_job_id))/'reducer-output'
        # Send job to worker
        self._send_reducing_job(
            input_files,
            reducer_executable,
            output_dir_reducer,
            worker_pid)
        # Update worker status
        self.worker_info['_workers'][worker_pid]['worker_state'] = \
            WorkerState.BUSY

    def _move_result_files(self):
        """Move reducer results to oringinal output directory."""
        current_job_id, current_job_msg = self._current_job
        output_dir = \
            self.info_dict['tmp']/("job-" +
                                   str(current_job_id))/"reducer-output"
        final_target_directory = Path(current_job_msg['output_directory'])

        # Create the output directory if it doesn’t already exist.
        if final_target_directory.exists():
            shutil.rmtree(final_target_directory)
        final_target_directory.mkdir()
        reducer_files = Path(output_dir).glob('*')

        # Copy files to oringinal output directory
        file_counter = 0
        for reducer_file in reducer_files:
            file_counter += 1
            if file_counter < 10:
                file_num = "0" + str(file_counter)
            else:
                file_num = str(file_counter)
            final_target_dir = final_target_directory/("outputfile" + file_num)
            shutil.copy(reducer_file, final_target_dir)

    # high level start and end=================================================
    def _start(self):
        """Start the manager."""
        self._udp_thread.start()
        self._tcp_listener()

    def _shutdown_workers(self):
        """Send shutdown msg to registered workers."""
        # for worker_pid, worker_info in self.worker_info['_workers'].items():
        for worker_info in self.worker_info['_workers'].values():
            if worker_info["worker_state"] != WorkerState.DEAD:
                worker = worker_info
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    # Bind the socket to the server
                    sock.connect((
                        worker["worker_host"],
                        worker["worker_port"]
                    ))
                    shutdown_msg = json.dumps({"message_type": "shutdown"})
                    sock.sendall(shutdown_msg.encode('utf-8'))

    def _shutdown(self):
        """Join thread and shutdown manager."""
        self._shutdown_workers()
        self._udp_thread.join()
        print('manager shutdown')


@click.command()
@click.argument("port_number", nargs=1, type=int)
@click.argument("hb_port_number", nargs=1, type=int)
def main(port_number, hb_port_number):
    """Start manager."""
    Manager(port_number, hb_port_number)


if __name__ == '__main__':
    main()
