server_addr = "https://jelka.fmf.uni-lj.si"

#docker
base_url = "unix:///var/run/docker.sock" #location of docker server
registry_url = "storzi.jakobkralj.com" #location of image regestry
mem_limit = "1g" #amount of ram that a pattern can consume
load_time = 10 # [s] how long before the current pattern stops should the next pattern load

pipe_location = "/tmp/jelka" #where should the pipe that accepts color data be located
