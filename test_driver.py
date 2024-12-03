import os, time

pipe_path = '/tmp/jelka'

framerate = 60 #default framerate
timedifference = 1e9 // 60 #ms
last_time = time.time_ns()

with open(pipe_path, 'r') as pipe:
    while True:
        # Read line from pipe
        print("pipe:", pipe.readline().strip())
