#!/usr/bin/python3
import requests
import socket
import sys
import datetime
import logging
import os

def setup_logging(log_level=logging.DEBUG, log_file=None):
    """
    Configure logging with optional file output and console logging.
    
    :param log_level: Logging level (default: logging.INFO)
    :param log_file: Optional path to log file
    """
    # Configure logging format
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[]
    )
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(logging.Formatter(log_format))
    logging.getLogger().addHandler(console_handler)
    
    # Add file handler if log_file is specified
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(logging.Formatter(log_format))
            logging.getLogger().addHandler(file_handler)
            logging.info(f"Logging to file: {log_file}")
        except IOError as e:
            logging.error(f"Could not create log file {log_file}: {e}")

def main():
    # Help message
    if len(sys.argv) < 3:
        logging.error(
            "Usage: %s endpoint bindport [bindhost6=any] [seconds/output=0] [logfile]\n"
            "Example: %s http://splet.4a.si/pub/1 6969 :: 1 service.log", 
            sys.argv[0], sys.argv[0]
        )
        sys.exit(1)
    
    # Optional log file
    log_file = sys.argv[5] if len(sys.argv) > 5 else None
    setup_logging(logging.DEBUG, log_file=log_file)
    
    # Log startup information
    logging.info(f"Starting UDP forwarding service")
    logging.info(f"Endpoint: {sys.argv[1]}")
    logging.info(f"Bind Port: {sys.argv[2]}")
    
    # Create requests session
    rs = requests.Session()
    
    # Create UDP socket
    try:
        ss = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        bind_host = sys.argv[3] if len(sys.argv) > 3 else ''
        bind_port = int(sys.argv[2])
        ss.bind((bind_host, bind_port))
        logging.info(f"Bound to [{bind_host}]:{bind_port}")
    except Exception as e:
        logging.error(f"Socket binding failed: {e}")
        sys.exit(1)
    
    # Output interval configuration
    output_interval = int(sys.argv[4]) if len(sys.argv) > 4 else 0
    last_output = datetime.datetime(2024, 12, 8, 15, 55, 6, 298861)
    
    # Main processing loop
    while True:
        try:
            # Receive UDP message
            message, address = ss.recvfrom(65536)
            logging.debug(f"Received message from {address}")
            
            # Forward message to endpoint
            try:
                resp = rs.post(sys.argv[1], data=message)
                logging.debug(f"Forwarded message. Response status: {resp.status_code}")
                
                # Periodic output handling
                current_time = datetime.datetime.now()
                if current_time - last_output > datetime.timedelta(seconds=output_interval):
                    last_output = current_time
                    logging.debug(f"Response content: {resp.text}")
            
            except requests.RequestException as req_err:
                logging.error(f"Request failed: {req_err}")
        
        except Exception as e:
            logging.error(f"Processing error: {e}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Service stopped by user")
        sys.exit(0)
