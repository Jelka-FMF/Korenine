import os, time

def create_pipe(path):
    """
    Create a named pipe at a specific filesystem location
    
    Args:
        path (str): Full path where pipe should be created
    """
    # Ensure directory exists
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    # Create pipe if it doesn't exist
    if not os.path.exists(path):
        os.mkfifo(path)
    
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
        print(pipe.readline().strip())
