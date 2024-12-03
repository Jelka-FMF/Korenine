import os, time

def create_pipe(path):
    """
    Create a named pipe at a specific filesystem location
    
    Args:
        path (str): Full path where pipe should be created
    """
    if os.path.exists(path):
        os.remove(path)  
    os.mkfifo(path, 0o666)
    
    return path

# Example usage
pipe_path = '/tmp/jelka'
created_pipe = create_pipe(pipe_path)
print(f"Pipe created at: {created_pipe}")

framerate = 60 #default framerate
timedifference = 1e9 // 60 #ms
last_time = time.time_ns()

with open(pipe_path, 'r') as pipe:
    while True:
        # Read line from pipe
        print("pipe:", pipe.readline().strip())
