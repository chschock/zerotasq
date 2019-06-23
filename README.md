# ZeroTasQ

This package is a very simple implementation of a load balancer leveraging PyZMQ to do the communication between processes. I tried to keep it as simple as possible. It is a simplified version of the load balancing broker from the ZMQ guide, that handles connections from multiple clients. Here we have only a single client, as usual in

## Which problem does it solve

The main drawback of Python is that the main interpreter, CPython, does not support threading of CPU load. So threads make your program concurrent and can also parallelize IO operation, but not computation. ZeroTasQ allows you to spawn processes from your main script / program, send them workload (from a single threading context, via a simple loop) or hand them over an iterable and receive a generator to iterate over the processed results.

The problem I had in mind when I wrote it was applying machine learning models in parallel in a streaming fashion. Part of the motivation was certainly to learn about ZeroMQ, which allows you to scale the library to a multi-machine context with minor changes.

You may want to use it if you have a data pipeline, where reading and writing can happen in a single process (unlike in real distributed frameworks) but you want the processing in between run parallel.

## How would you usually do it

The classic approach to do this is using Pool.map or Pool.imap from  the multiprocessing package. But this does not allow you to iterate over the results - so if you want to do stream processing, you would still have to chunk your data in some way. If your chunks are too small, the time to spawn a worker might becomes significant versus the processing time - especially if they load some big model. Big chunks waste memory on the other hand.

The main approach to parallel computing in a server context in Python is using a message queue like Redis or Celery together with worker processes. Those can be long-lived, such that their setup time is negligible, but the default worker model in rq spawns a child process for every task. You can override this though. You can easily solve the problem at hand with such a task queue, but you need a broker and have to spaws workers in some way. The latter could be done similarly to how it is done in ZeroTasQ.


## Design

To understand what is actually happening have a look at `example.py`.
The LoadBalancer context manager spawns a process for the LoadBalancing and the processes for the workers. Your are given a connector that allows you to send and receive tasks, where send is blocking if the queue is full and receive is blocking if the queue is empty. In simple cases you might want to use the `iter` method to simply feed all you tasks from an iterable into the LoadBalancer while you iterate over them to handle the results. In the manual scenario with `send` and `receive` you have two possibilities:
- mimic the approach the iter method uses
- start a sender and a receiver thread and join them after you have collected all the results.

