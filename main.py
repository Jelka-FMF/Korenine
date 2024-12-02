from typing import Annotated
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import Response
from sqlmodel import Field, Session, SQLModel, create_engine, select
from datetime import timedelta, datetime
import config
import docker
import asyncio, httpx
from typing import Optional
import time

engine = create_engine("sqlite:///database.db")
session = Session(engine) 
docker_client = docker.DockerClient(base_url = config.base_url)
registry = config.registry_url
mounts = [docker.types.Mount(config.pipe_location, config.pipe_location, type="bind")]

# Variable to store unscheduled direct runs from Jelkob
interruption = None

# Main class to store patterns for Jelka
class Pattern(SQLModel, table=True):
    identifier: str = Field(index=True, primary_key=True)
    name: str # Displayed name of pattern
    description: Optional[str] = None
    source: str 
    docker: str # Name of the docker image to be pulled from repository
    duration: Optional[timedelta] = Field(default=timedelta(seconds=60))
    author: str
    school: Optional[str] = None
    enabled: bool = True # Whether to display the pattern in normal rotation (direct runs are still possible)
    visible: bool = True # Whether the pattern is displayed on Jelkob
    changed: Optional[datetime] = Field(default=datetime.fromtimestamp(0)) #When was the pattern last changed (to be re-pulled)
    last_run: Optional[datetime] = Field(default=datetime.fromtimestamp(0)) #When was the pattern last run (Unix time epoch 0 denotes not being run yet at all)

def create_pattern_from_json(data):
    return { 
            'identifier' : data['identifier'],
        'name' : data['name'],
            'description' : data.get('description', ""),
            'source' : data.get('source'),
            'docker' : data['docker'],
            'duration' : timedelta(seconds=data['duration']),
            'author' : data['author'],
            'school' : data.get('school', ""),
            'enabled' : data.get('enabled', True),
            'visible' : data.get('visible', True),
            'changed' : datetime.fromisoformat(data.get('changed', datetime.fromtimestamp(0).isoformat())) 
            }
# Stores information for the pattern to be run directly
class Interruption:
    def __init__ (self, pattern_name):
        self.pattern_name = pattern_name # Name of pattern to be run
        self.pattern = sesion.select(Pattern).where(Pattern.identifier == pattern_name).first()
        self.image = createContainer(self.pattern.docker)


# Send Jelkob a ping every 30 seconds, so that Jelkob knows, that Korenine is still alive
async def send_ping(server_addr: str):
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{server_addr}/runner/state/ping")
        except Exception as e:
            print(f"Ping failed: {e}")

# Every 60 seconds it checks, if any new patterns are available
async def sync_patterns(server_addr: str):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{server_addr}/runner/patterns")
            remote_patterns = response.json()

            # Get existing patterns from local DB
            existing_patterns = session.exec(select(Pattern)).all()
            existing_identifiers = {p.identifier for p in existing_patterns}
            remote_identifiers = {p['identifier'] for p in remote_patterns}

            # Add or update patterns
            for pattern_data in remote_patterns:
                existing = session.exec(
                    select(Pattern).where(Pattern.identifier == pattern_data['identifier'])
                ).first()

                if existing:
                    # Update existing pattern
                    if pattern_data.changed > existing.changed:
                        docker_client.images.pull(pattern_data["docker"])
                    for key, value in create_pattern_from_json(pattern_data).items():
                        setattr(existing, key, value)
                else:
                    # Create new pattern
                    print(create_pattern_from_json(pattern_data))
                    new_pattern = Pattern(**create_pattern_from_json(pattern_data))
                    session.add(new_pattern)
                    docker_client.images.pull(pattern_data["docker"])

            # Remove patterns no longer in remote list
            for pattern in existing_patterns:
                if pattern.identifier not in remote_identifiers:
                    docker_client.images.remove(pattern["docker"])
                    session.delete(pattern)

            session.commit()

        except Exception as e:
            session.rollback()
            print(f"Pattern sync error: {e}")

def getNextPattern():
    earliest_item = session.exec(select(Pattern).order_by(Pattern.last_run).limit(1)).first() 
    if earliest_item:
        earliest_item.last_run = datetime.now()
        session.add(earliest_item)
        session.commit()
        return earliest_item, createContainer(earliest_item.docker)
    else:
        return None, None

def createContainer( container_name: str):
    image = docker_client.images.get(container_name)
    return docker_client.containers.create(image, 
                                  mem_limit = config.mem_limit,
                                  mounts = mounts,
                                  network_mode = "host",
                                  detach=True)

async def send_runner_state(server_addr: str, pattern_identifier: str):
    data = {
        "pattern": pattern_identifier,
        "started": datetime.now().isoformat()
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.post(f"{server_addr}/runner/state/started", json=data)
        return response

async def run_pattern():
    current_pattern = None
    current_container = None 
    next_pattern, next_container = getNextPattern()
    while True:
        current_pattern, current_container = next_pattern, next_container
        next_pattern, next_container = None, None
        if current_pattern == None:
            await asyncio.sleep(60)
            next_pattern, next_container = getNextPattern()
            continue
        start_time = time.time_ns()
        current_container.start()
        send_runner_state(config.server_addr, current_pattern.identifier)
        while time.time_ns() - start_time < current_pattern["duration"].seconds()*1e9  and not interruption:
            await asyncio.sleep(1)
            current_container.reload()

            if time.time_ns() - start_time + config.load_time*1e9 >= current_pattern["duration"].seconds()*1e9:
                next_pattern, next_container = getNextPattern()

            if not current_container.status == "running":
                next_pattern, next_container = getNextPattern()
                break
        current_container.kill()

        if interruption:
            next_pattern = interruption.pattern
            next_image = interruption.image
            interruption = None

def start_running_patterns(app: FastAPI):
    app.on_event("startup")
    async def startup_patterns():
        asyncio.create_task(run_pattern())



from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    async def pattern_sync_task():
       while True:
           try:
                await sync_patterns(config.server_addr)
           except Exception as e:
                print(f"Ping error: {e}")

           await asyncio.sleep(60)  # 1 minute
           print("ran sync")

    async def ping_task():
        while True:
            try:
                await send_ping(config.server_addr)
            except Exception as e:
                print(e) 
            await asyncio.sleep(30)
            print("ran ping")
    async def run_task():
        try:
            await run_pattern()
        except Exception as e:
            print(e)
    SQLModel.metadata.create_all(engine)
    sync = asyncio.create_task(pattern_sync_task())
    ping = asyncio.create_task(ping_task())
    run = asyncio.create_task(run_task())

    yield
    sync.cancel()
    ping.cancel()
    run.cancel()


app = FastAPI(lifespan=lifespan)

@app.post("/run")
async def create_interruption(identifier: str):
    global interruption
    interruption = Interruption(identifier)
    return Response(status_code=status.HTTP_200_OK)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
