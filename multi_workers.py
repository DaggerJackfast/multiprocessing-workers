"""
multiple mini workers
"""
import os
import time
import random
from multiprocessing import Process, Pipe, cpu_count, current_process
from scrapper.utils import clean_file

QUEUED = 'queued'
IN_PROGRESS = 'in_progress'
FINISHED = 'finished'


def run_worker(out_conn):
    worker_name = current_process().name
    print('[{}]---------'.format(worker_name))
    print('worker conn poll: {}'.format(out_conn.poll()))
    while True:
        if out_conn.poll():
            x, y, idx = out_conn.recv()
            if x == -1 and y == -1:
                print('worker {} stopped'.format(worker_name))
                break
            print('worker input: ({},{},{})'.format(x, y, idx))
            result = multiply(x, y)
            print('worker result for id: ', idx, ' res: ', result)
            # time.sleep(random.randint(1, 5))
            print('{} send result for id {} after wait'.format(worker_name, idx))
            out_conn.send((result, idx,))


def multiply(x, y):
    return x * y


def run_broker():
    cpus = cpu_count()
    worker_pool = []
    pipe_pool = []
    print('multi app started')
    for i in range(cpus):
        out_conn, in_conn = Pipe()
        name = 'worker-{}'.format(i+1)
        worker = Process(target=run_worker, name=name, args=(out_conn,),)
        worker_pool.append(worker)
        pipe_pool.append((out_conn, in_conn,))
        worker.daemon = True
        worker.start()
        task = get_free_task()
        send_task(in_conn, task)
        _start = time.time()
    runned_pipe = len(pipe_pool)
    while runned_pipe:
        for index, pipe in enumerate(pipe_pool):
            out_conn, in_conn = pipe
            print('[main]-------')
            print('pipe {} poll {}'.format(index, out_conn.poll()))
            if in_conn.poll():
                recvs = in_conn.recv()
                print('result from recv: ', recvs)
                result, idx = recvs
                print('Result is :', result)
                write_result(result)
                close_task(idx)
                task = get_free_task()
                if task is None:
                    in_conn.send((-1, -1, idx))
                    runned_pipe = runned_pipe - 1
                    continue

                send_task(in_conn, task)
    for pool in pipe_pool:
        in_conn, out_conn = pool
        out_conn.close()
        in_conn.close()

    for worker in worker_pool:
        worker.join()
        # worker.terminate()
    print("Sending numbers to Pipe() took {} seconds".format((time.time() - _start)))


def send_task(conn, task):
    x = task['x']
    y = task['y']
    idx = task['id']
    task['status'] = IN_PROGRESS
    conn.send((x, y, idx,))
    update_task(task)


def close_task(idx):
    update_task({'id': idx, 'status': FINISHED})


def get_free_task():
    tasks = read_tasks()
    for task in tasks:
        if task['status'] == QUEUED:
            return task
    return None


def update_task(updated_task):
    tasks = read_tasks()
    for task in tasks:
        if task['id'] == updated_task['id']:
            task.update(updated_task)
            break
    write_tasks(tasks)


def read_tasks():
    file = os.path.abspath('data/examples/tasks.txt')
    tasks = []
    with open(file, 'r') as task_file:
        for row in task_file:
            task_array = row.split('|')
            task = {
                'id': int(task_array[0]),
                'x': int(task_array[1]),
                'y': int(task_array[2]),
                'status': task_array[3].rstrip()
            }
            tasks.append(task)
    return tasks


def write_tasks(tasks):
    file = os.path.abspath('data/examples/tasks.txt')
    with open(file, 'w') as task_file:
        for task in tasks:
            idx = task['id']
            x = task['x']
            y = task['y']
            status = task['status']
            task_line = '{}|{}|{}|{}\n'.format(idx, x, y, status)
            task_file.write(task_line)


def write_result(result):
    file = os.path.abspath('data/examples/result.txt')
    with open(file, 'a+') as res_file:
        res_file.write('{}\n'.format(result))


def generate_random_tasks(count):
    tasks = []
    for i in range(count):
        task = {
            'id': i,
            'x': random.randint(1, 100),
            'y': random.randint(1, 100),
            'status': QUEUED
        }
        tasks.append(task)
    write_tasks(tasks)


if __name__ == "__main__":
    clean_file(os.path.abspath('data/examples/result.txt'))
    generate_random_tasks(300)
    run_broker()
