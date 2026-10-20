import threading

import numpy as np
import pylsl
import tomllib
import time
import pandas as pd
from fire import Fire

from consumer.utils.logging import logger


def connect_inlet(config: dict, msg_size: int, n_messages: int) -> pylsl.StreamInlet:
    """
    Resolve and connect to an input LSL stream.

    This function resolves a stream by name and initializes a native pylsl StreamInlet.

    Parameters
    ----------
    config : dict
        Configuration dictionary containing stream connection settings.

    Returns
    -------
    pylsl.StreamInlet
        Connected StreamInlet instance ready for data monitoring.
    """
    stream_name = config["stream_to_query"]["stream"]

    streams = pylsl.resolve_byprop("name", stream_name, timeout=pylsl.FOREVER)
    if not streams:
        raise RuntimeError(f"Could not resolve LSL stream with name '{stream_name}'")

    buffer_size = int(1000 * msg_size)
    print(f"Initializing inlet with buffer size: {buffer_size}")
    inlet = pylsl.StreamInlet(streams[0], max_buflen=buffer_size, max_chunklen=msg_size)
    inlet.open_stream(timeout=5)
    logger.debug(f"Connected to stream: {stream_name}")

    return inlet


def main(stop_event: threading.Event = threading.Event(), n_messages=1000, msg_size=1, output_file=""):
    """
    Main processing loop for the consumer.

    This function implements the main real-time processing loop for the consumer.
    It connects to an input LSL stream, receives and processes data, and writes
    timing results to a CSV file.
    The configuration is loaded from "./configs/consumer_config.toml".

    Parameters
    ----------
    stop_event : threading.Event, optional
        Threading event to signal when processing should stop. Default creates
        a new Event object.
    """
    logger.setLevel(10)
    config = tomllib.load(open("./configs/consumer_config.toml", "rb"))
    inlet = connect_inlet(config, msg_size, n_messages)
    print(f"Nominal rate of inlet: {inlet.info().nominal_srate()}")

    process_times = []
    recv_times = []
    counter = 0
    first_timestamp = None
    try:
        # Warmup loop - receive 500 samples before actual measurements
        while counter < 500 and not stop_event.is_set():
            sample, _ = inlet.pull_chunk(timeout=pylsl.FOREVER, max_samples=msg_size)
            counter += 1
            # print(f"Warmup message {counter}/500")

        # Reset counter for actual measurement
        counter = 0

        while counter < n_messages and not stop_event.is_set():
            sample, _ = inlet.pull_chunk(timeout=pylsl.FOREVER, max_samples=msg_size)
            now = time.monotonic()
            recv_times.append(now)
            data = sample[0][0]  # Extract the first element of the first sample
            if first_timestamp is None:
                first_timestamp = data
            process_times.append(now - data)
            counter += 1
            # print(f"Received message {counter}/{n_messages}")
    
    except KeyboardInterrupt:
        print("\n\nShutting down consumer...")
    finally:
        if len(recv_times) == 0:
            print("No messages received. Exiting without writing results.")
            inlet.close_stream()
            return
        
        print(f"Total messages processed: {len(process_times)}")

        recv_times_diff = np.diff(recv_times)
        mean_recv_period = np.mean(recv_times_diff)*1e6
        std_recv_period = np.std(recv_times_diff)*1e6
        max_recv_period = np.max(recv_times_diff)*1e6

        print(f"Average receive period (us): {mean_recv_period:.4f}")
        print(f"Max receive period (us): {max_recv_period:.4f}, idx: {np.argmax(recv_times_diff)}")
        print(f"Std receive period (us): {std_recv_period:.4f}")

        mean_latency = np.mean(process_times)*1e6
        std_latency = np.std(process_times)*1e6
        max_latency = np.max(process_times)*1e6

        print(f"Average latency (us): {mean_latency:.4f}")

        throughput = n_messages / (recv_times[-1] - first_timestamp)/1e3
        print(f"Throughput (msg/ms): {n_messages}/{(recv_times[-1] - first_timestamp)*1e3:.4f}ms={throughput:.2f}")

        results = pd.DataFrame({"Metric":["mean", "std", "max"],
                                "Recv_period": [mean_recv_period, std_recv_period, max_recv_period], 
                                "Latency": [mean_latency, std_latency, max_latency], 
                                "Throughput": [throughput, None, None]})

        results.to_csv(output_file, index=False)
        inlet.close_stream()
        # Persist per-packet process_times for post-processing
        try:
            base = output_file[:-4] if output_file.lower().endswith('.csv') else output_file
            proc_path = f"{base}_process_times.csv"
            with open(proc_path, "w") as f:
                for v in process_times:
                    f.write(f"{v}\n")
        except Exception:
            pass
        print("Consumer stopped.")


def get_main_thread() -> tuple[threading.Thread, threading.Event]:
    """
    Run the main processing loop in a separate thread.

    This function creates and starts a background thread that runs the main
    threshold controller loop. It allows the controller to be stopped via 
    the returned Event object.

    Returns
    -------
    tuple[threading.Thread, threading.Event]
        A tuple containing:
        - threading.Thread: The thread object running the controller loop
        - threading.Event: Event object that can be .set() to stop the controller
    """
    stop_event = threading.Event()
    stop_event.clear()

    thread = threading.Thread(target=main, kwargs={"stop_event": stop_event})
    thread.start()

    return thread, stop_event


if __name__ == "__main__":
    Fire(main)
