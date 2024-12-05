import logging
import traceback
from typing import Annotated, Optional
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.responses import Response
from sqlmodel import Field, Session, SQLModel, create_engine, select
from datetime import timedelta, datetime, timezone
import config
import docker
import asyncio, httpx
import time
import pytz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/jelka/Korenine/pattern_runner.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

utc = pytz.UTC

engine = create_engine("sqlite:///database.db")
session = Session(engine)
docker_client = docker.DockerClient(base_url=config.base_url)
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
    changed: Optional[datetime] = Field(default=utc.localize(datetime.fromtimestamp(0))) #When was the pattern last changed (to be re-pulled)
    last_run: Optional[datetime] = Field(default=datetime.fromtimestamp(0)) #When was the pattern last run (Unix time epoch 0 denotes not being run yet at all)

def create_pattern_from_json(data):
    logger.info(f"Creating pattern from JSON: {data['identifier']}")
    return {
        'identifier': data['identifier'],
        'name': data['name'],
        'description': data.get('description', ""),
        'source': data.get('source'),
        'docker': data['docker'],
        'duration': timedelta(seconds=data['duration']),
        'author': data['author'],
        'school': data.get('school', ""),
        'enabled': data.get('enabled', True),
        'visible': data.get('visible', True),
        'changed': datetime.fromisoformat(data.get('changed', utc.localize(datetime.fromtimestamp(0)).isoformat()))
    }

# Stores information for the pattern to be run directly
class Interruption:
    def __init__(self, pattern_name):
        self.pattern_name = pattern_name # Name of pattern to be run
        self.pattern = session.exec(select(Pattern).where(Pattern.identifier == pattern_name)).first()
        if not self.pattern:
            logger.error(f"Pattern not found: {pattern_name}")
            raise ValueError(f"Pattern {pattern_name} does not exist")
        self.image = createContainer(self.pattern.docker)
        logger.info(f"Interruption created for pattern: {pattern_name}")

def ensure_utc_timezone(dt):
    """
    Check if a datetime is timezone naive and add UTC timezone if so.
    If the datetime is already timezone-aware, return it as-is.
    
    Args:
        dt (datetime): The input datetime object
    
    Returns:
        datetime: A timezone-aware datetime in UTC
    """
    if dt.tzinfo is None:
        # If timezone is naive, localize to UTC
        return dt.replace(tzinfo=timezone.utc)
    
    # If already timezone-aware, return as-is
    return dt

# Send Jelkob a ping every 30 seconds, so that Jelkob knows, that Korenine is still alive
async def send_ping(server_addr: str):
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{server_addr}/runner/state/ping", headers={"Authorization": f"Token {config.TOKEN}"})
            logger.debug(f"Ping sent to {server_addr}")
        except Exception as e:
            logger.error(f"Ping failed: {e}")
            logger.debug(traceback.format_exc())

# Every 60 seconds it checks, if any new patterns are available
async def sync_patterns(server_addr: str):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{server_addr}/runner/patterns")
            remote_patterns = response.json()
            logger.info(f"Retrieved {len(remote_patterns)} remote patterns")

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
                    if ensure_utc_timezone(datetime.fromisoformat(pattern_data["changed"])) > ensure_utc_timezone(existing.changed):
                        logger.info(f"Updating pattern: {pattern_data['identifier']}")
                        docker_client.images.pull(pattern_data["docker"])
                    for key, value in create_pattern_from_json(pattern_data).items():
                        setattr(existing, key, value)
                else:
                    # Create new pattern
                    logger.info(f"Adding new pattern: {pattern_data['identifier']}")
                    new_pattern = Pattern(**create_pattern_from_json(pattern_data))
                    session.add(new_pattern)
                    docker_client.images.pull(pattern_data["docker"])

            # Remove patterns no longer in remote list
            for pattern in existing_patterns:
                if pattern.identifier not in remote_identifiers:
                    logger.info(f"Removing pattern: {pattern.identifier}")
                    docker_client.images.remove(pattern.docker)
                    session.delete(pattern)

            session.commit()
            logger.info("Pattern synchronization completed successfully")

        except Exception as e:
            session.rollback()
            logger.error(f"Pattern sync error: {e}")
            logger.debug(traceback.format_exc())

def getNextPattern():
    earliest_item = session.exec(select(Pattern).where(Pattern.enabled == True).order_by(Pattern.last_run).limit(1)).first()
    if earliest_item:
        earliest_item.last_run = datetime.now()
        session.add(earliest_item)
        session.commit()
        logger.info(f"Selected next pattern: {earliest_item.identifier}")
        return earliest_item, createContainer(earliest_item.docker)
    else:
        logger.warning("No patterns available to run")
        return None, None

def createContainer(container_name: str):
    try:
        image = docker_client.images.get(container_name)
        container = docker_client.containers.create(
            image,
            mem_limit=config.mem_limit,
            mounts=mounts,
            network_mode="host",
            detach=True
        )
        logger.info(f"Created container for image: {container_name}")
        return container
    except Exception as e:
        logger.error(f"Failed to create container for {container_name}: {e}")
        raise

async def send_runner_state(server_addr: str, pattern_identifier: str):
    data = {
        "pattern": pattern_identifier,
        "started": datetime.now().isoformat()
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(f"{server_addr}/runner/state/started", json=data, headers={"Authorization": f"Token {config.TOKEN}"})
            logger.info(f"Sent runner state for pattern: {pattern_identifier}")
            return response
        except Exception as e:
            logger.error(f"Failed to send runner state for {pattern_identifier}: {e}")
            raise

async def run_pattern():
    global interruption
    current_pattern = None
    current_container = None
    next_pattern, next_container = getNextPattern()
    while True:
        try:
            current_pattern, current_container = next_pattern, next_container
            next_pattern, next_container = None, None
            
            if current_pattern is None:
                logger.info("No pattern available, sleeping for 60 seconds")
                await asyncio.sleep(60)
                next_pattern, next_container = getNextPattern()
                continue

            start_time = time.time_ns()
            current_container.start()
            try:
                await send_runner_state(config.server_addr, current_pattern.identifier)
            except:
                pass
            logger.info(f"Started running pattern: {current_pattern.identifier}")
            
            while (time.time_ns() - start_time < current_pattern.duration.seconds*1e9 and 
                   not interruption):
                await asyncio.sleep(1)
                current_container.reload()

                if (time.time_ns() - start_time + config.load_time*1e9 >= 
                    current_pattern.duration.seconds*1e9) and next_pattern == None:
                    next_pattern, next_container = getNextPattern()

                if not current_container.status == "running":
                    logger.warning(f"Container for {current_pattern.name} stopped. Status: {current_container.status}")
                    logger.debug(f"Container logs: {current_container.logs()}")
                    next_pattern, next_container = getNextPattern()
                    break

            if current_container.status == "running":
                current_container.kill()
                logger.info(f"Killed container for pattern: {current_pattern.identifier}")

            if interruption:
                logger.info(f"Handling interruption for pattern: {interruption.pattern_name}")
                next_pattern = interruption.pattern
                next_container = interruption.image
                interruption = None

        except Exception as e:
            logger.error(f"Error in run_pattern: {e}")
            logger.debug(traceback.format_exc())
            await asyncio.sleep(10)  # Prevent tight error loop

def kill_all_docker_containers():
    for container in docker_client.containers.list():
        container.kill()
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    async def pattern_sync_task():
       while True:
           try:
                await sync_patterns(config.server_addr)
                logger.debug("Pattern sync completed")
           except Exception as e:
                logger.error(f"Pattern sync error: {e}")
                logger.debug(traceback.format_exc())

           await asyncio.sleep(60)  # 1 minute

    async def ping_task():
        while True:
            try:
                await send_ping(config.server_addr)
                logger.debug("Ping task completed")
            except Exception as e:
                logger.error(f"Ping task error: {e}")
                logger.debug(traceback.format_exc())
            await asyncio.sleep(30)

    async def run_task():
        while True:
            try:
                kill_all_docker_containers()
                await run_pattern()
            except Exception as e:
                logger.error(f"Run task error: {e}")
                logger.debug(traceback.format_exc())

    SQLModel.metadata.create_all(engine)
    sync = asyncio.create_task(pattern_sync_task())
    ping = asyncio.create_task(ping_task())
    run = asyncio.create_task(run_task())

    yield
    
    # Cancel tasks on shutdown
    sync.cancel()
    ping.cancel()
    run.cancel()
    logger.info("Application shutdown initiated")

app = FastAPI(lifespan=lifespan)

@app.post("/run")
async def create_interruption(identifier: str):
    global interruption
    try:
        interruption = Interruption(identifier)
        logger.info(f"Interruption created for pattern: {identifier}")
        return Response(status_code=status.HTTP_200_OK)
    except ValueError as e:
        logger.error(f"Failed to create interruption: {e}")
        raise HTTPException(status_code=404, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting Pattern Runner application")
    uvicorn.run(app, host="0.0.0.0", port=8080)
