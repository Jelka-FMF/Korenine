import os
from rpi_ws281x import PixelStrip, Color
import json
import time

def decode_header(header: str) -> dict:
    """Decodes a header string into a dictionary.
    Almost the reverse of encode_header. Does not check for correct types.
    Raises a ValueError if the header does not contain a version or if the version is not supported.

    When making header for your personal use just hardcode the version number.
    Older versions will be supported as long as possible.

    Examples:
    >>> header = {"led_count": 500, "fps": 60}
    >>> decode_header(encode_header(**header)) == dict(header, version=0)
    True
    """
    json_header = json.loads(header)
    if "version" not in json_header:
        raise ValueError("Header must contain a version.")

    if json_header["version"] == 0:
        if not all(key in json_header for key in ("led_count", "fps")):
            raise ValueError("Header (version 0) must contain led_count and fps.")
    else:
        raise ValueError(f"Unsupported header version: {json_header['version']}.")

    return json_header

def decode_frame(frame: str, led_count: int, version: int) -> list:
    """Decodes a frame string into a list of rgb tuples.
    The frame string must have 6 characters per led, 2 for each rgb value.
    Will raise a ValueError if the frame is not valid.

    If format ever changes so will the version number. This will allow for backwards compatibility.

    Examples:
    >>> decode_frame('0001020304050096ff', 3, version=0)
    [(0, 1, 2), (3, 4, 5), (0, 150, 255)]
    """

    assert version in (0,), f"Unsupported frame version: {version}."
    if not isinstance(frame, str):
        raise TypeError(f"Expected type 'str', found type {type(frame)}.")

    expected_length = 3 * led_count * 2  # 3 * 2 characters per led
    if len(frame) != expected_length:
        raise ValueError(f"Frame has wrong size, expected exactly {expected_length} bytes, found {len(frame)}.")

    return [
        (int(frame[i : i + 2], 16), int(frame[i + 2 : i + 4], 16), int(frame[i + 4 : i + 6], 16))
        for i in range(0, len(frame), 6)
    ]
# Example usage
pipe_path = '/tmp/jelka'

LED_PIN = 18					# GPIO pin connected to the pixels (18 uses PWM!).
LED_FREQ_HZ = 800000	# LED signal frequency in hertz (usually 800khz)
LED_DMA = 10					# DMA channel to use for generating signal (try 10)
LED_BRIGHTNESS = 255	# Set to 0 for darkest and 255 for brightest
LED_INVERT = False		# True to invert the signal (when using NPN transistor level shift)
LED_CHANNEL = 0				# set to '1' for GPIOs 13, 19, 41, 45 or 53
LED_NUMBER = 500 # number of led-s set

framerate = 60 #default framerate
timedifference = 1e9 // 60 #ms
last_time = time.time_ns()
strip = PixelStrip(LED_NUMBER, LED_PIN, LED_FREQ_HZ, LED_DMA, LED_INVERT, LED_BRIGHTNESS, LED_CHANNEL)
strip.begin()

with open(pipe_path, 'r') as pipe:
    while True:
        # Read line from pipe
        line = pipe.readline().strip()

        if not line or line[0] != "#":
            continue
        if line[1] == "{":
            header = decode_header(line[1:])
            framerate = header["fps"]
            timedifference = 1e9 // framerate
            continue

        frame = decode_frame(line[1:], LED_NUMBER, 0)
        for i, pixel in enumerate(frame):
            #Color scheme is GRB not RGB
            strip[i] = Color(pixel[1], pixel[0], pixel[2])
        if time.time_ns() - last_time < timedifference:
            time.sleep((time.time_ns() - last_time)/1e9)
        strip.show()
        last_time = time.time_ns()
