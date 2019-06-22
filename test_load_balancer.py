from zerotasq import LoadBalancer, Worker
import time
import random
import pytest

N_TASKS = 1000
random.seed(0)

class EchoWorker(Worker):
    def process(self, msg):
        # time.sleep(random.random() * 10)
        return msg


@pytest.fixture
def workers():
    return [EchoWorker() for i in range(8)]

@pytest.fixture
def complete_tasks():
    return ["{:05d}".format(i) for i in range(N_TASKS)]


def test_async_complete(workers, complete_tasks):
    complete_tasks_iterator = iter(complete_tasks)
    with LoadBalancer(workers) as conn:
        results = list(conn.iter(complete_tasks_iterator))

    assert len(set(results)) == N_TASKS


def test_sync_complete(workers, complete_tasks):
    print(complete_tasks)
    complete_tasks_iterator = iter(complete_tasks)
    with LoadBalancer(workers, reply_sync=True) as conn:
        results = list(conn.iter(complete_tasks_iterator))

    assert len(results) == N_TASKS
    assert results == complete_tasks


