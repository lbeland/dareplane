import threading

import numpy as np
import pylsl
import tomllib
import time
from dareplane_utils.general.time import sleep_s
from dareplane_utils.stream_watcher.lsl_stream_watcher import (
    StreamWatcher,
    pylsl_xmlelement_to_dict,
)
from fire import Fire

from consumer.utils.logging import logger
import base64


def connect_stream_watcher(config: dict) -> StreamWatcher:
    """
    Connect to and configure the input stream watcher.

    This function initializes a StreamWatcher to monitor an input LSL stream.
    If the outlet frequency is set to "derive", the frequency is set equal to the
    nominal sampling rate of the input stream.

    Parameters
    ----------
    config : dict
        Configuration dictionary containing stream connection settings.

    Returns
    -------
    StreamWatcher
        Connected StreamWatcher instance ready for data monitoring.
    """
    sw = StreamWatcher(
        config["stream_to_query"]["stream"],
        buffer_size_s=config["stream_to_query"]["buffer_size_s"],
    )
    sw.connect_to_stream()
    logger.debug(f"Connected to stream: {config['stream_to_query']['stream']}")

    # if the outlet config is to be derived, calc from here
    if config["lsl_outlet"]["nominal_freq_hz"] == "derive":
        inlet_info = pylsl_xmlelement_to_dict(sw.inlet.info())
        config["lsl_outlet"]["nominal_freq_hz"] = float(
            inlet_info["info"]["nominal_srate"]
        )
        logger.debug(f"Derived outlet nominal_freq_hz: {config['lsl_outlet']['nominal_freq_hz']}")

    return sw


def main(stop_event: threading.Event = threading.Event(), n_messages=1000, msg_size=1, n_channels=1,output_file=""):
    """
    Main processing loop for the threshold controller.

    This function implements the main real-time processing loop for the threshold
    controller. It connects to an input LSL stream, applies threshold-based decisions
    to the incoming data, and streams the control outputs via an LSL outlet.
    The configuration is loaded from "./configs/threshold_controller_config.toml".

    Parameters
    ----------
    stop_event : threading.Event, optional
        Threading event to signal when processing should stop. Default creates
        a new Event object.
    """
    logger.setLevel(10)
    config = tomllib.load(open("./configs/consumer_config.toml", "rb"))
    sw = connect_stream_watcher(config)

    process_times = []
    recv_times = []
    counter = 0
    chunk_counter = 0
    try:
        # Receive one sample that is not counted to initialize the stream      
        while counter < n_messages + 1:
            sw.update()

            if sw.n_new > 0:
                # Receive message
                ufbuffer = sw.unfold_buffer()[-sw.n_new :]
                # print(f"Received: {data}")
                for sample in ufbuffer:
                    chunk_counter += 1
                    if chunk_counter == msg_size:
                        now = time.monotonic()
                        chunk_counter = 0
                        recv_times.append(now)
                        data = sample[0]
                        process_times.append(now - data)
                        counter+=1
                sw.n_new = 0
    
    except KeyboardInterrupt:
        print("\n\nShutting down consumer...")
    
    finally:
        print(f"Total messages processed: {len(process_times)}")
        process_times = process_times[1:]     # exclude first measuremnt because of startup delay
        # print(f"Average processing time (s): {np.mean(process_times)/1e9:.6f}")
        # print(f"Max processing time (s): {np.max(process_times)/1e9:.6f}, idx: {np.argmax(process_times)}")
        # print(f"Std processing time (s): {np.std(process_times)/1e9:.6f}")
        # print('\n')
        recv_times = recv_times[1:]
        recv_times_diff = np.diff(recv_times)
        print(f"Average receive period (s): {np.mean(recv_times_diff):.6f}")
        print(f"Max receive period (s): {np.max(recv_times_diff):.6f}, idx: {np.argmax(recv_times_diff)}")
        print(f"Std receive period (s): {np.std(recv_times_diff):.6f}")

        save = np.vstack((recv_times, process_times, np.append(recv_times_diff, 0)))
        np.savetxt(output_file, save.T, delimiter=',', header='recv_time,latency,recv_period')
        sw.inlet.close_stream()
        sw.disconnect()
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
