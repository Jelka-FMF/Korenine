import csv
import math
import curses
import numpy as np
import os

class LightController:
    def __init__(self, csv_file="positions.csv", pipe_path="/tmp/jelka"):
        self.csv_file = csv_file  # Store the CSV filename for saving later
        self.positions = []
        self.pipe_path = pipe_path
        self.load_positions(csv_file)
        self.rotation_angle = 0
        self.is_mirrored = False
        
    def load_positions(self, csv_file):
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            self.positions = []
            for row in reader:
                # Convert all values to float except the index
                i, x, y, z = int(row[0]), float(row[1]), float(row[2]), float(row[3])
                self.positions.append([i, x, y, z])
        self.positions = np.array(self.positions)

    def save_positions(self):
        # Create backup of original file
        backup_file = f"{self.csv_file}.backup"
        try:
            if not os.path.exists(backup_file):
                os.rename(self.csv_file, backup_file)
        except OSError:
            pass  # If backup creation fails, proceed anyway
            
        # Save current positions to CSV
        with open(self.csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            for position in self.positions:
                # Round floating point numbers to 6 decimal places for cleaner output
                writer.writerow([int(position[0])] + [round(x, 6) for x in position[1:]])
        return f"Positions saved to {self.csv_file}"
        
    def rotate_positions(self, angle_delta):
        self.rotation_angle += angle_delta
        # Create rotation matrix
        cos_theta = math.cos(math.radians(angle_delta))
        sin_theta = math.sin(math.radians(angle_delta))
        rotation_matrix = np.array([
            [cos_theta, -sin_theta, 0],
            [sin_theta, cos_theta, 0],
            [0, 0, 1]
        ])
        
        # Apply rotation to x, y, z coordinates
        coords = self.positions[:, 1:4]
        rotated_coords = np.dot(coords, rotation_matrix.T)
        self.positions[:, 1:4] = rotated_coords
        
    def mirror_positions(self):
        self.is_mirrored = not self.is_mirrored
        # Mirror around YZ plane (flip X coordinates)
        self.positions[:, 1] = -self.positions[:, 1]
        
    def get_light_color(self, x, y):
        if x < 0:
            if y < 0:
                return 0xFF0000  # Red
            else:
                return 0xFF00FF  # Purple
        else:
            if y < 0:
                return 0x00FF00  # Green
            else:
                return 0x0000FF  # Blue
                
    def update_lights(self):
        # Create color array based on positions
        colors = []
        for _, x, y, _ in self.positions:
            color = self.get_light_color(x, y)
            colors.append(color)
            
        # Format the output string
        output = "#" + "".join(f"{color:06x}" for color in colors) + "\n"
        
        # Write to pipe
        try:
            with open(self.pipe_path, 'w') as pipe:
                pipe.write(output)
        except IOError as e:
            return f"Error writing to pipe: {e}"
        return "Lights updated successfully"

def main(stdscr):
    # Initialize curses
    curses.curs_set(0)
    stdscr.nodelay(1)
    stdscr.timeout(100)
    
    # Initialize controller
    controller = LightController()
    
    # Display initial status
    stdscr.addstr(0, 0, "Light Control TUI")
    stdscr.addstr(2, 0, "Controls:")
    stdscr.addstr(3, 0, "← → : Rotate")
    stdscr.addstr(4, 0, "m : Mirror")
    stdscr.addstr(5, 0, "q : Quit and save")
    
    while True:
        # Get user input
        try:
            key = stdscr.getch()
        except:
            key = -1
            
        if key == ord('q'):
            # Save positions before quitting
            save_status = controller.save_positions()
            stdscr.addstr(10, 0, save_status)
            stdscr.refresh()
            curses.napms(1000)  # Show save status for 1 second
            break
        elif key == curses.KEY_LEFT:
            controller.rotate_positions(-5)  # Rotate 5 degrees left
            status = controller.update_lights()
        elif key == curses.KEY_RIGHT:
            controller.rotate_positions(5)   # Rotate 5 degrees right
            status = controller.update_lights()
        elif key == ord('m'):
            controller.mirror_positions()
            status = controller.update_lights()
            
        # Update status display
        stdscr.addstr(7, 0, f"Rotation: {controller.rotation_angle}°  ")
        stdscr.addstr(8, 0, f"Mirrored: {'Yes' if controller.is_mirrored else 'No'}  ")
        stdscr.refresh()

if __name__ == "__main__":
    # Create pipe if it doesn't exist
    if not os.path.exists("/tmp/jelka"):
        os.mkfifo("/tmp/jelka")
        
    # Start the application
    curses.wrapper(main)
