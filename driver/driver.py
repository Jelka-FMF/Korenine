import sys
import threading
import queue
import os
import time
import socket

def decode_frame(frame: str, led_count: int) -> list:
    """Decodes a frame string into a list of rgb tuples.
    The frame string must have 6 characters per led, 2 for each rgb value.
    """
    expected_length = 3 * led_count * 2  # 3 * 2 characters per led
    if len(frame) != expected_length:
        raise ValueError(f"Frame has wrong size, expected exactly {expected_length} bytes, found {len(frame)}.")

    return [
        (int(frame[i : i + 2], 16), int(frame[i + 2 : i + 4], 16), int(frame[i + 4 : i + 6], 16))
        for i in range(0, len(frame), 6)
    ]

def device_writer(dev_path, q, done_event):
    """Worker thread that continuously writes frames to a device"""
    while True:
        pixels = q.get()
        if pixels is None:  # Poison pill to stop thread
            break
        
        # Build entire frame buffer at once
        # Try different byte orders - original code had GRB order
        frame_data = bytearray()
        for pixel in pixels:
            # Try GRB order (like WS2812 LEDs)
            frame_data.extend([pixel[1], pixel[0], pixel[2], 0x00])
        
        # Try to write, with timeout
        max_retries = 5
        for attempt in range(max_retries):
            try:
                with open(dev_path, 'wb') as dev:
                    dev.write(frame_data)
                break  # Success
            except OSError as e:
                if attempt < max_retries - 1:
                    time.sleep(0.01)  # 10ms between retries
        
        done_event.set()

# Configuration
pipe_path = '/tmp/jelka'
LED_NUMBER = 1000  # Total number of LEDs
LEDS_PER_DEVICE = 500  # 500 LEDs per device

ss = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
ss.bind(("", 0))

# Create queues and events for synchronization
queue0 = queue.Queue(maxsize=1)
queue1 = queue.Queue(maxsize=1)
done0 = threading.Event()
done1 = threading.Event()

# Start worker threads
thread0 = threading.Thread(target=device_writer, args=('/dev/leds0', queue0, done0), daemon=True)
thread1 = threading.Thread(target=device_writer, args=('/dev/leds1', queue1, done1), daemon=True)
thread0.start()
thread1.start()

with open(pipe_path, 'r') as pipe:
    while True:
        # Read line from pipe
        line = pipe.readline()
        if not line:  # EOF
            time.sleep(0.1)
            continue

        ss.sendto(b"event: message\ndata: " + line.encode() + b"\n", "65536")
        line = line.strip()
        # Skip lines that don't start with #
        if not line or line[0] != "#":
            continue

        # Skip header lines (starting with #{)
        if line[1] == "{":
            continue

        # Decode frame
        frame = decode_frame(line[1:], LED_NUMBER)
        
        # Split frame into two halves
        pixels_dev0 = frame[:LEDS_PER_DEVICE]
        pixels_dev1 = frame[LEDS_PER_DEVICE:]
        
        # Clear done events
        done0.clear()
        done1.clear()
        
        # Send pixels to worker threads
        queue0.put(pixels_dev0)
        queue1.put(pixels_dev1)
        
        # Wait for both writes to complete
        done0.wait()
        done1.wait()
