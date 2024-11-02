# main.py
from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlmodel import Field, Session, SQLModel, create_engine, select
from typing import List, Optional
import os
from datetime import datetime
import bcrypt
import secrets
from datetime import datetime

# Database Models
class Pattern(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    identifier: str = Field(max_length=50, unique=True, index=True)
    name: str = Field(max_length=255)
    docker: str = Field(max_length=255)
    duration: Optional[int] = Field(default=None)
    author: Optional[str] = Field(default=None, max_length=255)
    school: Optional[str] = Field(default=None, max_length=255)
    enabled: bool = Field(default=True)
    last_run: datetime = Field(
        sa_column=Column(DateTime(timezone=True), server_default=datetime.utcfromtimestamp(0))
    ) 

class APIKey(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    key_hash: str = Field(index=True)
    prefix: str = Field(max_length=8)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = Field(default=True)
    description: Optional[str] = Field(default=None, max_length=255)

# Database Configuration
DATABASE_URL = "sqlite:///./patterns.db"
engine = create_engine(DATABASE_URL)

# Security
security = HTTPBearer()

class APIKeyManager:
    @staticmethod
    def generate_key() -> tuple[str, str]:
        """Generate a new API key and its prefix"""
        key = secrets.token_hex(32)
        prefix = key[:8]
        return key, prefix

    @staticmethod
    def hash_key(key: str) -> str:
        """Hash the API key using bcrypt"""
        return bcrypt.hashpw(key.encode(), bcrypt.gensalt()).decode()

    @staticmethod
    def verify_key(key: str, key_hash: str) -> bool:
        """Verify if a key matches its hash"""
        try:
            return bcrypt.checkpw(key.encode(), key_hash.encode())
        except Exception:
            return False

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

# API Key verification
def verify_api_key(credentials: HTTPAuthorizationCredentials = Security(security)):
    key = credentials.credentials
    prefix = key[:8]
    
    with Session(engine) as session:
        stored_key = session.exec(
            select(APIKey)
            .where(APIKey.prefix == prefix, APIKey.is_active == True)
        ).first()
        
        if not stored_key or not APIKeyManager.verify_key(key, stored_key.key_hash):
            raise HTTPException(
                status_code=401,
                detail="Invalid or expired API key"
            )
        return stored_key

app = FastAPI(title="Pattern API")

@app.on_event("startup")
async def on_startup():
    create_db_and_tables()

# Pattern Endpoints
@app.post("/patterns/", response_model=Pattern)
def create_pattern(
    pattern: Pattern,
    current_key: APIKey = Depends(verify_api_key)
):
    """Create a new pattern"""
    with Session(engine) as session:
        existing = session.exec(
            select(Pattern).where(Pattern.identifier == pattern.identifier)
        ).first()
        if existing:
            raise HTTPException(
                status_code=400,
                detail="Pattern with this identifier already exists"
            )
        
        session.add(pattern)
        session.commit()
        session.refresh(pattern)
        return pattern

@app.get("/patterns/", response_model=List[Pattern])
def read_patterns(
    skip: int = 0,
    limit: int = 100,
    enabled: Optional[bool] = None,
    current_key: APIKey = Depends(verify_api_key)
):
    """Get all patterns with optional filtering"""
    with Session(engine) as session:
        query = select(Pattern)
        if enabled is not None:
            query = query.where(Pattern.enabled == enabled)
        patterns = session.exec(query.offset(skip).limit(limit)).all()
        return patterns

@app.get("/patterns/{pattern_id}", response_model=Pattern)
def read_pattern(
    pattern_id: int,
    current_key: APIKey = Depends(verify_api_key)
):
    """Get a specific pattern by ID"""
    with Session(engine) as session:
        pattern = session.get(Pattern, pattern_id)
        if not pattern:
            raise HTTPException(status_code=404, detail="Pattern not found")
        return pattern

@app.get("/patterns/by-identifier/{identifier}", response_model=Pattern)
def read_pattern_by_identifier(
    identifier: str,
    current_key: APIKey = Depends(verify_api_key)
):
    """Get a specific pattern by identifier"""
    with Session(engine) as session:
        pattern = session.exec(
            select(Pattern).where(Pattern.identifier == identifier)
        ).first()
        if not pattern:
            raise HTTPException(status_code=404, detail="Pattern not found")
        return pattern

@app.put("/patterns/{pattern_id}", response_model=Pattern)
def update_pattern(
    pattern_id: int,
    pattern_update: Pattern,
    current_key: APIKey = Depends(verify_api_key)
):
    """Update a pattern"""
    with Session(engine) as session:
        pattern = session.get(Pattern, pattern_id)
        if not pattern:
            raise HTTPException(status_code=404, detail="Pattern not found")
        
        if pattern_update.identifier != pattern.identifier:
            existing = session.exec(
                select(Pattern).where(Pattern.identifier == pattern_update.identifier)
            ).first()
            if existing:
                raise HTTPException(
                    status_code=400,
                    detail="Pattern with this identifier already exists"
                )
        
        pattern_data = pattern_update.dict(exclude_unset=True)
        for key, value in pattern_data.items():
            setattr(pattern, key, value)
        
        session.add(pattern)
        session.commit()
        session.refresh(pattern)
        return pattern

@app.delete("/patterns/{pattern_id}")
def delete_pattern(
    pattern_id: int,
    current_key: APIKey = Depends(verify_api_key)
):
    """Delete a pattern"""
    with Session(engine) as session:
        pattern = session.get(Pattern, pattern_id)
        if not pattern:
            raise HTTPException(status_code=404, detail="Pattern not found")
        
        session.delete(pattern)
        session.commit()
        return {"message": "Pattern deleted"}
