"""
This script is designed to interact with SQLiteCloud, read data from a temperature sensor, and detect motion using a PIR sensor on a Raspberry Pi.

It performs the following functions:
1. Connects to the SQLiteCloud database using credentials provided via environment variables.
2. Creates necessary tables (`motion` and `temperature`) in the database if they do not already exist.
3. Reads temperature data from a W1ThermSensor.
4. Detects motion using a PIR sensor connected to GPIO pin 23.
5. Inserts temperature and motion data into the SQLiteCloud database with timestamps.
6. Continuously reads sensor data and updates the database until interrupted by the user.

Modules:
- RPi.GPIO: Controls the GPIO pins on the Raspberry Pi.
- w1thermsensor: Interfaces with the temperature sensor.
- sqlitecloud: Connects to the SQLiteCloud database.
- datetime: Handles timestamps.
- time: Provides sleep functionality.
- os: Loads environment variables.

Environment Variables:
- API_KEY: The API key for accessing SQLiteCloud.
- DATABASE_URL: The URL to the SQLiteCloud database.
- DB_NAME: The name of the database within SQLiteCloud.

Usage:
1. Set environment variables for API_KEY, DATABASE_URL, and DB_NAME on your Raspberry Pi.
2. Run this script on the Raspberry Pi.

Example:
    python your_script_name.py

Exceptions:
- ValueError: Raised if any required environment variable is missing.
- Exception: Raised for general errors such as failure to connect to SQLiteCloud or issues during database operations.

"""
import RPi.GPIO as GPIO
from w1thermsensor import W1ThermSensor
import sqlitecloud
from datetime import datetime
import time
import os

# Load environment variables
API_KEY = os.getenv('API_KEY')
DATABASE_URL = os.getenv('DATABASE_URL')
DB_NAME = os.getenv('DB_NAME')

# Check if environment variables are loaded
if not API_KEY or not DATABASE_URL or not DB_NAME:
    raise ValueError("Missing environment variables for API_KEY, DATABASE_URL, or DB_NAME")

# Open the connection to SQLite Cloud
try:
    conn = sqlitecloud.connect(f"{DATABASE_URL}/{DB_NAME}?apikey={API_KEY}")
    print("Connected to SQLite Cloud")
except Exception as e:
    print(f"Failed to connect to SQLite Cloud: {e}")
    exit()

def create_tables_if_not_exists():
    """
    Creates the 'motion' and 'temperature' tables in the SQLiteCloud database if they do not exist.

    Tables:
    - motion: Contains columns for id, timestamp, and motion_detected.
    - temperature: Contains columns for id, timestamp, and temperature.
    """
    try:
        create_motion_table_query = '''
        CREATE TABLE IF NOT EXISTS motion (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            motion_detected INTEGER NOT NULL
        )
        '''
        create_temperature_table_query = '''
        CREATE TABLE IF NOT EXISTS temperature (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            temperature REAL NOT NULL
        )
        '''
        conn.execute(create_motion_table_query)
        conn.execute(create_temperature_table_query)
        conn.commit()
        print('Tables are ready')
    except Exception as e:
        print(f"Failed to create tables: {e}")

def insert_motion(motion_detected, timestamp):
    """
    Inserts a record into the 'motion' table.

    Args:
    motion_detected (int): Indicates whether motion was detected (1) or not (0).
    timestamp (str): ISO formatted timestamp of the record.
    """
    try:
        insert_query = '''
        INSERT INTO motion (timestamp, motion_detected)
        VALUES (?, ?)
        '''
        conn.execute(insert_query, (timestamp, motion_detected))
        conn.commit()
        print(f'Record inserted into motion table: {timestamp}, Motion Detected: {motion_detected}')
    except Exception as e:
        print(f'An error occurred while inserting into motion table: {e}')

def insert_temperature(temperature, timestamp):
    """
    Inserts a record into the 'temperature' table.

    Args:
    temperature (float): The temperature reading in Celsius.
    timestamp (str): ISO formatted timestamp of the record.
    """
    try:
        insert_query = '''
        INSERT INTO temperature (timestamp, temperature)
        VALUES (?, ?)
        '''
        conn.execute(insert_query, (timestamp, temperature))
        conn.commit()
        print(f'Record inserted into temperature table: {timestamp}, Temperature: {temperature:.2f}')
    except Exception as e:
        print(f'An error occurred while inserting into temperature table: {e}')

# Initialize the sensor
sensor = W1ThermSensor()

# GPIO setup
PIR_PIN = 23
GPIO.setmode(GPIO.BCM)
GPIO.setup(PIR_PIN, GPIO.IN)

# Ensure the tables are ready
create_tables_if_not_exists()

try:
    print("System Test (CTRL+C to exit)")
    time.sleep(2)
    print("Ready")

    # Loop to keep the script running
    while True:
        try:
            # Read temperature
            temperature_celsius = sensor.get_temperature()
            print(f"Temperature: {temperature_celsius:.2f} °C")
            
            # Read motion
            motion_state = GPIO.input(PIR_PIN)

            # Get the current timestamp
            timestamp = datetime.now().isoformat()

            # Insert data into both tables with the same timestamp
            insert_temperature(temperature_celsius, timestamp)
            insert_motion(motion_state, timestamp)

            # Print motion status
            if motion_state:
                print("Motion detected!")
            else:
                print("No motion")

        except Exception as e:
            print(f'Error: {e}')

        time.sleep(1)

except KeyboardInterrupt:
    print("Quit")
finally:
    GPIO.cleanup()
    conn.close()
    print("Cleaned up GPIO and closed database connection.")
