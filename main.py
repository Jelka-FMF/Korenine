from typing import Annotated
from fastapi import Depends, FastAPI, HTTPException, Query
from sqlmodel import Field, Session, SQLModel, create_engine, select
from datetime import timedelta, datetime
import config
import docker

class Pattern(SQLModel, table=True):
    identifier: str = Field(index=True, primary_key=True)
    name: str
    description: Optional[str] = None
    source: str
    docker: str
    duration: timedelta
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

class DockerDatabase:
    def __init__(self, engine, session: Session, config):
        self.engine = engine
        self.session = session
        self.client = docker.DockerClient(base_url = config.base_url)
        self.registry = config.registry_url

    async def sync_patterns(self, server_addr: str):
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(f"{server_addr}/runner/patterns")
                remote_patterns = response.json()

                # Get existing patterns from local DB
                existing_patterns = self.session.exec(select(Pattern)).all()
                existing_identifiers = {p.identifier for p in existing_patterns}
                remote_identifiers = {p['identifier'] for p in remote_patterns}

                # Add or update patterns
                for pattern_data in remote_patterns:
                    existing = self.session.exec(
                        select(Pattern).where(Pattern.identifier == pattern_data['identifier'])
                    ).first()

                    if existing:
                        # Update existing pattern
                        for key, value in pattern_data.items():
                            setattr(existing, key, value)
                    else:
                        # Create new pattern
                        new_pattern = Pattern(**pattern_data)
                        self.session.add(new_pattern)

                # Remove patterns no longer in remote list
                for pattern in existing_patterns:
                    if pattern.identifier not in remote_identifiers:
                        self.session.delete(pattern)

                self.session.commit()

            except Exception as e:
                self.session.rollback()
                print(f"Pattern sync error: {e}")
    def getNextPattern():
        earliest_item = session.exec(select(Pattern).order_by(Pattern.last_run).limit(1)).first() 
        if earliest_item:
            earliest_item.last_run = datetime.now()
            session.add(earliest_item)
            session.commit()
        return earliest_item

def start_periodic_pattern_sync(app: FastAPI, database_sync: DatabaseSync, server_addr: str):
    async def pattern_sync_task():
        while True:
            await database_sync.sync_patterns(server_addr)
            await asyncio.sleep(60)  # 1 minute
    
    @app.on_event("startup")
    async def startup_event():
        asyncio.create_task(pattern_sync_task())
