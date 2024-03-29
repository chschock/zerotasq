#!/usr/bin/python
import sys
import time
import click
import random
from zerotasq import LoadBalancer, Worker


class ExampleWorker(Worker):
    """Parallelize random sleeping."""

    def init(self, max_duration):
        """
        You can load libraries or models here.
        """
        self.max_duration = max_duration

    def process(self, duration):
        """
        This is executed for every incoming task.
        :param duration: duration to sleep in ms
        :return: status bar string how long 'calculation' took
        """
        time.sleep(duration / 1000)
        return "#" * min(100, round(duration / self.max_duration * 100))


Q_SIZE = 200


@click.command()
@click.option("--max-duration", default=100, help="max sleep duration in ms")
@click.option("--n-tasks", default=200, type=click.INT)
@click.option("--n-proc", default=8, type=click.INT)
@click.option("--sync/--async", is_flag=True, default=True)
@click.option("--loglevel", default=1, type=click.Choice([0, 1, 2, 3]))
def example(max_duration, n_tasks, n_proc, sync, loglevel):

    workers = [
        ExampleWorker(init_kwargs={"max_duration": max_duration}, loglevel=loglevel)
        for _ in range(n_proc)
    ]

    tasks = (random.random() * max_duration for _ in range(n_tasks))

    with LoadBalancer(workers, reply_sync=sync, loglevel=loglevel) as conn:
        for result in conn.iter(tasks, cache_size=Q_SIZE):
            print(result)


if __name__ == "__main__":
    example()
