import redis
import time
import random

id = 0

pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
red = redis.Redis(connection_pool=pool)
red.set('oldFib', 1)
red.set('fib', 1)


class Manager:
    def __init__(self, M):
        self.M = M

    def startWorker(self, Mg):
        currTime = int(time.perf_counter())
        if currTime - Mg > self.M:
            w = Worker(5, 10, 20, 10, id)
            w.initWorker()


class Worker:
    def __init__(self, N, T, W, C, id):
        self.N = N
        self.T = T
        self.W = W
        self.C = C
        self.id = id
        id += 1

    def initWorker(self):
        currTime = int(time.perf_counter())
        workers = red.zrange('heartbeats', 0, -1, withscores=True)

        for work in workers:
            if work[1] < currTime - self.T:
                red.zrem('heartbeats', work[0])

        if len(workers) < self.N:
            red.zadd('heartbeats', {self.id: currTime})
            self.startProcess()

    def mightCrash(self):
        currC = int(time.perf_counter())
        while True:
            currTime = int(time.perf_counter())
            if currTime - currC < self.C:
                chance = random.randint(0, 10)
                if chance == 1:
                    red.zrem('heartbeats', self.id)
                currC = currTime

    def updateHeartbeat(self):
        currT = int(time.perf_counter())
        while True:
            currTime = int(time.perf_counter())
            if currTime - currT < self.T:
                red.zadd('heartbeats', {self.id: currTime})
                currT = currTime

    def work(self):
        currW = int(time.perf_counter())
        while True:
            currTime = int(time.perf_counter())
            if currTime - currW < self.W:
                newFib = int(red.get('fib')) + int(red.get('oldFib'))
                red.set('oldFib', red.get('fib'))
                red.set('fib', newFib)
                currW = currTime

    def startProcess(self):
        self.updateHeartbeat()
        self.mightCrash()
        self.work()


# manager
# starts worker every m seconds

# worker
# zset heartbeats : score->last beat, member->id
# delete outdated elem  (score < (now() - T) seconds)
# if less than N, add worker else exit
# every t seconds, worker should update heartbeat
# every w seconds, worker should work
# every c seconds, a woker has 10% chance to crash
# work: replace current with the next fibo term

M = Manager(30)
while True:
    Mg = int(time.perf_counter())
    M.startWorker(Mg)
