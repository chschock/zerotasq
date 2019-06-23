#!/usr/bin/python
import logging
import abc
import pickle
import time
import random
import zmq
from multiprocessing import Process, Event
from itertools import islice
from termcolor import colored

LOG_LEVELS = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]


def set_logger(context, level=1):
    logger = logging.getLogger(context)
    logger.setLevel(LOG_LEVELS[level])
    formatter = logging.Formatter(
        "%(levelname)-.1s:%(msecs).6s:"
        + context
        + ":[%(funcName).5s:%(lineno)3d]:%(message)s",
        datefmt="%m-%d %H:%M:%S",
    )
    console_handler = logging.StreamHandler()
    console_handler.setLevel(LOG_LEVELS[level])
    console_handler.setFormatter(formatter)
    logger.handlers = []
    logger.addHandler(console_handler)
    return logger


class Worker(Process, abc.ABC):
    """
    Abstract worker class. You implement this class by adding a `process` method, which
    contains the workload to be applied to a single task. The `init` method is executed
    once when the worker process is spawned and should include heavy setup operations
    like loading a model from disk or building up some necessary datastructure.
    The worker process executes a simple while loop alternating between receiving tasks
    and sending results, processing them in between. At startup it registers at the
    LoadBalancer by sending 'READY'.
    """

    def __init__(self, init_kwargs: dict = {}, loglevel: int = 1):
        super().__init__()
        self.daemon = True
        self.init_kwargs = init_kwargs
        self.loglevel = loglevel

    def setup(self, worker_id: str, backend_con: str):
        self.worker_id = worker_id
        self.backend_con = backend_con
        self.logger = set_logger(colored(worker_id, "yellow"), self.loglevel)

    def init(self, **kwargs):
        """Abstract method to initialize the worker. Called by run as we want setup to run
        parallel."""
        pass

    @abc.abstractmethod
    def process(self, request):
        """Abstract method to process a task. Called in `run`."""
        ...

    def close(self):
        self.logger.info("shutting down...")
        self.terminate()
        self.join()
        self.logger.info("terminated!")

    def run(self):
        context = zmq.Context()
        backend = context.socket(zmq.REQ)
        backend.identity = self.worker_id.encode("ascii")
        backend.connect(self.backend_con)
        self.init(**self.init_kwargs)
        backend.send(b"READY")

        def loop():
            while True:
                self.logger.debug("waiting msg")
                req_id, request = backend.recv_pyobj()
                self.logger.debug("received msg id %d", req_id)
                reply = self.process(request)
                self.logger.debug("sending msg id %d", req_id)
                backend.send_pyobj((req_id, reply))

        try:
            loop()
        except KeyboardInterrupt:
            pass

        self.logger.warning("terminating worker {}".format(self.worker_id))
        for socket in [backend]:
            socket.linger = 0
            socket.close()
        context.term()


class LoadBalancer(Process):
    """
    Load Balancer inspired by load balancing broker from ZeroMQ guide (
    http://zguide.zeromq.org/py:lbbroker). While the original handles multiple clients,
    we have only The model is a simple source (PULL) sink (PUSH) with fan-out fan-in to
    connect to the workers via a backend socket (ROUTER).
    """

    def __init__(self, workers: Worker, reply_sync: bool = False, loglevel: int = 1):
        super().__init__()
        self.daemon = True
        self.workers = workers
        self.reply_sync = reply_sync
        self.is_ready = Event()
        self.id = "lb-{:05d}".format(random.randint(0, 100000))
        self.source_con = "ipc://@{}-source".format(self.id)
        self.sink_con = "ipc://@{}-sink".format(self.id)
        self.backend_con = "ipc://@{}-backend".format(self.id)
        self.control_con = "ipc://@{}-control".format(self.id)
        self.logger = set_logger(colored(self.id, "green"), level=loglevel)

        # setup background workers
        for i, w in enumerate(self.workers):
            assert not w.is_alive()
            worker_id = "{}-wkr-{:02d}".format(self.id, i)
            w.setup(worker_id, self.backend_con)

    def __enter__(self):
        self.start()
        for w in self.workers:
            w.start()
        self.is_ready.wait()
        self.starttime = time.time()
        return Connector(self, self.source_con, self.sink_con)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.info("ran %5.3f seconds", time.time() - self.starttime)
        self.close()

    def close(self):
        self.logger.info("shutting down...")
        self.is_ready.clear()
        for w in self.workers:
            w.close()
            w.join()
        self.terminate()
        self.join()
        self.logger.info("terminated!")

    def run(self):
        context = zmq.Context.instance()
        source = context.socket(zmq.PULL)
        source.connect(self.source_con)
        sink = context.socket(zmq.PUSH)
        sink.bind(self.sink_con)
        backend = context.socket(zmq.ROUTER)
        backend.bind(self.backend_con)
        control = context.socket(zmq.REP)
        control.connect(self.control_con)

        self.logger.info("collect workers")
        worker_queue = []
        for _, p in enumerate(self.workers):
            worker, empty, req_id = backend.recv_multipart()
            worker_queue.append(worker)

        # poll for requests from backend and source
        poller = zmq.Poller()
        poller.register(backend, zmq.POLLIN)
        poller.register(source, zmq.POLLIN)
        poller.register(control, zmq.POLLIN)

        queue = dict()

        self.is_ready.set()

        def loop():
            source_msg_id, sink_msg_id = 0, 0
            while True:
                self.logger.debug("polling")
                sockets = dict(poller.poll())

                if control in sockets:
                    self.logger.info("control received")
                    pass

                if backend in sockets:
                    # Handle worker activity on the backend
                    worker, _, pkl = backend.recv_multipart()
                    self.logger.debug("message from backend %s", worker.decode())
                    reply_id, reply = pickle.loads(pkl)
                    if not worker_queue:
                        # start polling source as one worker just became available
                        poller.register(source, zmq.POLLIN)
                    worker_queue.append(worker)
                    if not self.reply_sync:
                        sink.send_pyobj(reply)
                    else:
                        # store result in queue
                        queue[reply_id] = reply
                        # dequeue leading replies
                        while sink_msg_id in queue:
                            reply = queue.pop(sink_msg_id)
                            sink.send_pyobj(reply)
                            sink_msg_id += 1

                if source in sockets:
                    self.logger.debug("message from source")
                    # Get next request, route to last-used worker
                    request = source.recv_pyobj()
                    worker = worker_queue.pop(0)
                    backend.send_multipart(
                        [worker, b"", pickle.dumps((source_msg_id, request))]
                    )
                    source_msg_id += 1
                    if not worker_queue:
                        # Don't poll source if no workers are available
                        poller.unregister(source)

        try:
            loop()
        except KeyboardInterrupt:
            pass

        for socket in [backend, source, sink, control]:
            socket.linger = 0
            socket.close()
        context.term()


QUEUE_SIZE = 1000


class Connector:
    def __init__(self, lb, source_con: str, sink_con: str):
        self.lb = lb
        self.context = zmq.Context()
        self.sender = self.context.socket(zmq.PUSH)
        self.sender.setsockopt(zmq.SNDHWM, QUEUE_SIZE)
        self.sender.bind(source_con)
        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.setsockopt(zmq.RCVHWM, QUEUE_SIZE)
        self.receiver.connect(sink_con)

    def send(self, task):
        """
        Send a task.
        :param task: task to enqueue
        """
        self.sender.send_pyobj(task)

    def receive(self):
        """
        Receive a result.
        :return: next result
        """
        return self.receiver.recv_pyobj()

    def iter(self, iterable, cache_size=100):
        """
        Enqueue tasks from iterable into LoadBalancer pre-filling the queue with
        `cache_size` tasks.

        :param iterable: iterable of tasks
        :param cache_size: size of task stock in queue
        :return: generator of result
        """
        cache_size_max, cache_size = cache_size, 0
        for task in islice(iterable, cache_size_max):
            self.send(task)
            cache_size += 1

        self.lb.logger.info("queue filled")

        while True:
            try:
                task = next(iterable)
            except StopIteration as e:
                break
            self.send(task)
            yield self.receive()
        self.lb.logger.info("all tasks sent")

        for _ in range(cache_size):
            yield self.receive()
        self.lb.logger.info("queue emptied")

    def __del__(self):
        for socket in [self.sender, self.receiver]:
            socket.linger = 0
            socket.close()
        self.context.term()
