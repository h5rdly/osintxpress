import time
import logging
from osintxpress import OsintEngine

# Configure Python's standard logging to catch the Rust tracing events!
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')


def main():
    
    print("--- Initializing Engine ---")
    engine = OsintEngine(worker_threads=2)
    
    engine.add_rest_source("dummy_acled", "http://localhost:8080/events", 5)
    
    print("\n--- Starting Engine ---")
    engine.start()
    
    # Let Python sleep. You should see Rust background ticks printing to the console!
    time.sleep(3.5)
    
    print("\n--- Polling Data ---")
    # This will trigger the py.allow_threads block
    data = engine.poll()
    print(f"Data received: {type(data)} -> {data}")
    
    print("\n--- Stopping Engine ---")
    engine.stop()


if __name__ == "__main__":

    main()