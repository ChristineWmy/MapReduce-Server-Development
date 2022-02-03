"""Example threading."""
import os
import threading


def main():
    """Test Threading."""
    threads = []
    for i in range(5):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()


def worker(worker_id):
    """Worker thread."""
    pid = os.getpid()
    print("Worker {}  pid={}".format(worker_id, pid))
    while True:
        # spin
        pass


if __name__ == "__main__":
    main()