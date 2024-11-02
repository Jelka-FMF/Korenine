# manage_keys.py
import click
from sqlmodel import Session, create_engine, select
from main import APIKey, APIKeyManager, DATABASE_URL
import sys
from tabulate import tabulate
from datetime import datetime

engine = create_engine(DATABASE_URL)

@click.group()
def cli():
    """Management script for API keys"""
    pass

@cli.command()
@click.option('--description', '-d', help='Description for the API key')
def create(description: str):
    """Create a new API key"""
    key, prefix = APIKeyManager.generate_key()
    key_hash = APIKeyManager.hash_key(key)
    
    api_key = APIKey(
        key_hash=key_hash,
        prefix=prefix,
        description=description
    )
    
    with Session(engine) as session:
        session.add(api_key)
        session.commit()
        
        # Print the key information
        click.echo("\nNew API Key created:")
        click.echo(f"Key: {key}")
        click.echo(f"Prefix: {prefix}")
        click.echo(f"Description: {description}")
        click.echo("\nWARNING: Store this key securely! It cannot be retrieved later.")

@cli.command()
def list():
    """List all API keys"""
    with Session(engine) as session:
        keys = session.exec(select(APIKey)).all()
        
        if not keys:
            click.echo("No API keys found.")
            return
        
        # Prepare data for tabulate
        headers = ["ID", "Prefix", "Created At", "Active", "Description"]
        data = [
            [
                key.id,
                key.prefix,
                key.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "Yes" if key.is_active else "No",
                key.description or ""
            ]
            for key in keys
        ]
        
        click.echo("\nAPI Keys:")
        click.echo(tabulate(data, headers=headers, tablefmt="grid"))

@cli.command()
@click.argument('key_id', type=int)
def deactivate(key_id: int):
    """Deactivate an API key"""
    with Session(engine) as session:
        key = session.get(APIKey, key_id)
        if not key:
            click.echo(f"No API key found with ID {key_id}")
            sys.exit(1)
            
        key.is_active = False
        session.add(key)
        session.commit()
        click.echo(f"API key {key_id} has been deactivated.")

if __name__ == '__main__':
    cli()
