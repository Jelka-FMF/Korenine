from typing import Annotated
from fastapi import Depends, FastAPI, HTTPException, Query
from sqlmodel import Field, Session, SQLModel, create_engine, select
from datetime import timedelta, datetime
import config
import docker

app = FastAPI()
engine = create_engine("sqlite:///database.db")
sesion = Session(engine) 
client = docker.DockerClient(base_url = config.base_url)
registry = config.registry_url
mounts = [docker.Mount(config.pipe_location, config.pipe_location)]


class Pattern(SQLModel, table=True):
    identifier: str = Field(index=True, primary_key=True)
    name: str
    description: Optional[str] = None
    source: str
    docker: str
    duration: Optional[timedelta] = Field(default=timedelta(seconds=60))
    author: str
    school: Optional[str] = None
    enabled: bool = True
    visible: bool = True
    last_run: Optional[datetime] = Field(default=datetime.fromtimestamp(0))

def create_pattern_from_json(data: Dict[str, Any]) -> Sphere:
    return Pattern(
        identifier=data['identifier'],
        name=data['name'],
        description=data.get('description', ""),
        source=data.get('source'),
        docker=data['docker'],
        duration=timedelta(seconds=data['duration']),
        author=data['author'],
        school=data.get('school', ""),
        enabled=data.get('enabled', True),
        visible=data.get('visible', True)
    )

@app.on_event("startup")
def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

async def send_ping(server_addr: str):
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{server_addr}/runner/state/ping")
        except Exception as e:
            print(f"Ping failed: {e}")

def start_periodic_ping(app: FastAPI, server_addr: str):
    async def ping_task():
        while True:
            await send_ping(server_addr)
            await asyncio.sleep(30)
    
    @app.on_event("startup")
    async def startup_event():
        asyncio.create_task(ping_task())

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
                    for key, value in pattern_data.items():
                        setattr(existing, key, value)
                else:
                    # Create new pattern
                    new_pattern = Pattern(**pattern_data)
                    session.add(new_pattern)
                    client.images.pull(registry + pattern_data["docker"])

            # Remove patterns no longer in remote list
            for pattern in existing_patterns:
                if pattern.identifier not in remote_identifiers:
                    client.images.remove(pattern["docker"])
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
    return earliest_item
def createContainer( container_name: str):
    image = client.images.get(container_name)
    return client.containers.create(image, 
                                  mem_limit = config.mem_limit,
                                  mounts = mounts,
                                  network_mode = "host",
                                  detach=True)

async def run_pattern():
    current_pattern = None
    current_container = None 
    next_pattern = getNextPattern()
    next_container = createContainer()
    while True:
        current_pattern, current_container = next_pattern, next_container
        next_pattern, next_container = None, None
        start_time = time.time_ns()
        current_container.start()
        while time.time_ns() - start_time < current_pattern["duration"].seconds()*1e9 and current_container.status == "running" :
            await asynco.sleep(1)
            current_container.reload()
            if time.time_ns() - start_time + config.load_time*1e9 >= current_pattern["duration"].seconds()*1e9:
                next_pattern = getNextPatter()
                next_container = createContainer()
        current_container.kill()

def start_periodic_pattern_sync(app: FastAPI, database_sync: DatabaseSync, server_addr: str):
    async def pattern_sync_task():
       while True:
           await database_sync.sync_patterns(server_addr)
           await asyncio.sleep(60)  # 1 minute
    
    app.on_event("startup")
    async def startup_event():
       asyncio.create_task(pattern_sync_task())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
