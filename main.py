import logging
import traceback
from typing import Optional
import json
import asyncio
import httpx
import time
import pytz
from datetime import timedelta, datetime, timezone
from sqlmodel import Field, Session, SQLModel, create_engine, select
from config import config
import docker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/jelka/Korenine/logs/pattern_runner.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

utc = pytz.UTC

engine = create_engine("sqlite:///data/database.db")
session = Session(engine)
docker_client = docker.DockerClient(base_url=config.base_url)
docker_client.login(config.username, config.password, registry=config.registry_url)
registry = config.registry_url
mounts = [
    docker.types.Mount(config.pipe_location, config.pipe_location, type="bind"), 
    docker.types.Mount("/app/positions.csv", config.position_location, type="bind", read_only=True)
]
environment = {"JELKA_POSITIONS": "/app/positions.csv"}

# Variable to store unscheduled direct runs from Jelkob
interruption = None

# Main class to store patterns for Jelka
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
    changed: Optional[datetime] = Field(default=utc.localize(datetime.fromtimestamp(0)))
    last_run: Optional[datetime] = Field(default=datetime.fromtimestamp(0))

def create_pattern_from_json(data):
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

def createContainer(container_name: str):
    try:
        image = docker_client.images.get(container_name)
        container = docker_client.containers.create(
            image,
            mem_limit=config.mem_limit,
            mounts=mounts,
            network_mode="host",
            environment=environment,
            detach=True
        )
        logger.info(f"Created container for image: {container_name}")
        return container
    except Exception as e:
        logger.error(f"Failed to create container for {container_name}: {e}")
        raise

# Stores information for the pattern to be run directly
class Interruption:
    def __init__(self, pattern_name):
        self.pattern_name = pattern_name 
        self.pattern = session.exec(select(Pattern).where(Pattern.identifier == pattern_name)).first()
        if not self.pattern:
            logger.error(f"Pattern not found: {pattern_name}")
            raise ValueError(f"Pattern {pattern_name} does not exist")
        self.image = createContainer(self.pattern.docker)
        logger.info(f"Interruption created for pattern: {pattern_name}")

def ensure_utc_timezone(dt):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt

# Core logic to update database from a list of pattern dictionaries
def process_patterns_update(remote_patterns):
    try:
        logger.info(f"Processing {len(remote_patterns)} remote patterns")

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
                try:
                    docker_client.images.remove(pattern.docker, force=True)
                except Exception as e:
                    logger.warning(f"Could not remove docker image {pattern.docker}: {e}")
                session.delete(pattern)

        session.commit()
        logger.info("Pattern synchronization completed successfully")

    except Exception as e:
        session.rollback()
        logger.error(f"Pattern update error: {e}")
        logger.debug(traceback.format_exc())

# Initial fetch via standard HTTP GET
async def fetch_initial_patterns(server_addr: str):
    async with httpx.AsyncClient() as client:
        try:
            logger.info("Performing initial pattern fetch...")
            response = await client.get(f"{server_addr}/runner/patterns")
            response.raise_for_status()
            remote_patterns = response.json()
            process_patterns_update(remote_patterns)
        except Exception as e:
            logger.error(f"Initial pattern fetch failed: {e}")

# Long-running task to listen to SSE
async def listen_to_events(server_addr: str):
    global interruption
    url = f"{server_addr}/runner/events/control"
    headers = {
        "Authorization": f"Token {config.TOKEN}",
        "Accept": "text/event-stream"
    }

    logger.info(f"Starting SSE listener on {url}")
    
    retry_delay = 5

    while True:
        try:
            async with httpx.AsyncClient(timeout=None) as client:
                async with client.stream("GET", url, headers=headers) as response:
                    if response.status_code != 200:
                        logger.error(f"SSE connection failed with status {response.status_code}")
                        await asyncio.sleep(retry_delay)
                        continue
                    
                    logger.info("SSE Stream connected")
                    current_event = None
                    
                    async for line in response.aiter_lines():
                        if not line:
                            continue
                            
                        if line.startswith("event:"):
                            current_event = line.replace("event:", "").strip()
                        elif line.startswith("data:"):
                            data_str = line.replace("data:", "").strip()
                            
                            # Handle empty data lines
                            if not data_str:
                                continue

                            try:
                                if current_event == "run":
                                    data = json.loads(data_str)
                                    identifier = data.get("identifier")
                                    if identifier:
                                        logger.info(f"Received RUN event for {identifier}")
                                        try:
                                            interruption = Interruption(identifier)
                                        except Exception as ie:
                                            logger.error(f"Failed to process interruption: {ie}")
                                
                                elif current_event == "patterns":
                                    logger.info("Received PATTERNS update event")
                                    data = json.loads(data_str)
                                    # Run DB updates in a thread to not block the async loop deeply
                                    await asyncio.to_thread(process_patterns_update, data)
                                    
                            except json.JSONDecodeError:
                                logger.error(f"Failed to decode JSON from event: {current_event}")
                            except Exception as e:
                                logger.error(f"Error processing event {current_event}: {e}")
                                logger.debug(traceback.format_exc())
                                
        except httpx.RequestError as e:
            logger.error(f"SSE connection error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in SSE listener: {e}")
            logger.debug(traceback.format_exc())
        
        logger.info(f"Reconnecting SSE in {retry_delay} seconds...")
        await asyncio.sleep(retry_delay)

# Send Jelkob a ping every 30 seconds
async def send_ping(server_addr: str):
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{server_addr}/runner/state/ping", headers={"Authorization": f"Token {config.TOKEN}"})
            logger.debug(f"Ping sent to {server_addr}")
        except Exception as e:
            logger.error(f"Ping failed: {e}")

async def ping_loop(server_addr: str):
    while True:
        await send_ping(server_addr)
        await asyncio.sleep(30)

def getNextPattern(current_pattern = None):
    if not current_pattern:
        earliest_item = session.exec(select(Pattern).where(Pattern.enabled == True).order_by(Pattern.last_run).limit(1)).first()
        if earliest_item:
            earliest_item.last_run = datetime.now()
            session.add(earliest_item)
            session.commit()
            logger.info(f"Selected next pattern by time: {earliest_item.identifier}")
            return earliest_item, createContainer(earliest_item.docker)
        else:
            logger.warning("No patterns available to run")
            return None, None
    else: 
        # Find the next pattern alphabetically after the current pattern
        next_pattern = session.exec(
            select(Pattern)
            .where(
                Pattern.enabled == True, 
                Pattern.identifier > current_pattern.identifier
            )
            .order_by(Pattern.identifier)
            .limit(1)
        ).first()
        
        # If no next pattern, loop back to the first pattern alphabetically
        if not next_pattern:
            next_pattern = session.exec(
                select(Pattern)
                .where(Pattern.enabled == True)
                .order_by(Pattern.identifier)
                .limit(1)
            ).first()
        
        if next_pattern:
            next_pattern.last_run = datetime.now()
            session.add(next_pattern)
            session.commit()
            logger.info(f"Selected next pattern by letter: {next_pattern.identifier}")
            return next_pattern, createContainer(next_pattern.docker)
        else:
            logger.warning("No patterns available to run")
            return None, None

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
                    next_pattern, next_container = getNextPattern(current_pattern)

                if not current_container.status == "running":
                    logger.warning(f"Container for {current_pattern.name} stopped. Status: {current_container.status}")
                    logger.warning(f"Container logs: {current_container.logs()}")
                    if next_pattern == None:
                        next_pattern, next_container = getNextPattern(current_pattern)
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
    try:
        for container in docker_client.containers.list():
            container.kill()
    except Exception as e:
        logger.error(f"Error killing containers: {e}")

async def main():
    logger.info("Starting Pattern Runner application")
    
    # Initialize DB
    SQLModel.metadata.create_all(engine)
    
    # Clean slate
    kill_all_docker_containers()

    # Initial Sync
    await fetch_initial_patterns(config.server_addr)

    # Create background tasks
    # 1. Ping Task
    ping_task = asyncio.create_task(ping_loop(config.server_addr))
    
    # 2. SSE Listener Task
    sse_task = asyncio.create_task(listen_to_events(config.server_addr))
    
    # 3. Main Runner Loop (running directly in main await)
    try:
        await run_pattern()
    except asyncio.CancelledError:
        logger.info("Main runner cancelled")
    finally:
        ping_task.cancel()
        sse_task.cancel()
        kill_all_docker_containers()
        logger.info("Shutdown complete")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
