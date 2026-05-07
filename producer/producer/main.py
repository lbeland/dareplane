# Stream from files defined in the config/streaming.toml

import threading  # necessary to make it accessible via api

# Safeguard against older python versions
try:
    import tomllib
except ModuleNotFoundError:
    print(
        "It seems you are using a python version < 3.11. Please install the `tomli` package via pip for toml parsing."
    )
    import tomli as tomllib

from pathlib import Path, PureWindowsPath

import numpy as np
import pylsl
import time
import pandas as pd
from fire import Fire

from producer.utils.logging import logger

class MockupStream:
    """A mockup streamer class to represent data from one source
    (file or random). Each such stream can have up to one markers stream
    associated which will be streamed in parallel with a separate name

    Attributes
    ----------
    name : str
        Name of the stream
    sfreq : float
        Target sampling frequency
    outlet : pylsl.StreamOutlet
        pylsl StreamOutlet object data is pushed to
    outlet_mrk : pylsl.StreamOutlet | None
        pylsl StreamOutlet object markers are pushed to
    buffer : np.ndarray
        The pre buffered data to be streamed from
    buffer_i : int
        index of the current position in the data buffer
    n_pushed : int
        number of samples pushed
    t_start_s : float
        timestamp of the start of the stream, required samples will be
        calculated relative to this

    """

    def __init__(
        self,
        name: str,
        cfg: dict,
        files: list[Path] = [],
        n_messages: int = None,
    ):
        """
        Parameters
        ----------
        files : list[Path] (optional)
            if provided, data will be streamed from files, else random data
            will be generateed
        """
        self.name = name
        self.cfg = cfg
        self.files = files  # can be used if multiple files should be
        self.file_i = 0
        self.outlet = None
        self.sfreq = pylsl.IRREGULAR_RATE #cfg["sampling_freq"]
        self.msg_size = cfg["msg_size"]
        self.n_messages = n_messages

        self.num_channels = cfg.get("n_channels", 1)
        self.buffer = None

        # LSL - init after first data is loaded, as sfreq might be derived
        self.init_outlet()

    def init_buffer(self, data, markers: np.ndarray | None = None):
        self.buffer = data
        self.markers = markers
        self.buffer_i = 0
        self.n_pushed = 0
        self.t_start_s = pylsl.local_clock()

    def init_outlet(self):

        # set the stream type depending on data in the buffer
        fmt_map = {
            "float32": pylsl.cf_float32,
            "float64": pylsl.cf_double64,
            "string": pylsl.cf_string,
            "int32": pylsl.cf_int32,
            "int16": pylsl.cf_int16,
            "int8": pylsl.cf_int8,
            "int64": pylsl.cf_int64,
        }
        dtype = fmt_map.get(self.cfg.get("data_type", "float64"), "float64")

        print(f"fs of mockstream: {self.sfreq}, dtype: {dtype}")

        info = pylsl.StreamInfo(
            self.cfg["stream_name"],
            self.cfg.get("stream_type", "EEG"),
            self.num_channels,
            self.sfreq,
            channel_format=dtype,
        )

        self.info = info
        buffer_size = int(1000*self.msg_size)
        print(f"Initializing outlet with buffer size: {buffer_size}")
        self.outlet = pylsl.StreamOutlet(self.info, chunk_size=self.msg_size,max_buffered=buffer_size)
        print(f"Nominal rate of outlet: {self.outlet.get_info().nominal_srate()}")


def run_stream(
    stop_event: threading.Event = threading.Event(),
    conf_pth: Path = Path("./config/streams.toml"),
    n_messages: int = None,
    n_channels: int = None,
    msg_size: int=None,
    t_wait: float = None,
    output_file: str = "send_times.csv",
) -> int:
    """
    Parameters
    ----------
    stop_event : threading.Event
        event used to stop the streaming
    conf_pth : Path
        Path to config file
    n_messages : int, optional
        Number of messages to send (overrides config)
    n_channels : int, optional
        Number of channels (overrides config)
    msg_size : int, optional
        Size of each message in samples (overrides config)
    t_wait : float, optional
    Returns
    -------
    int
        returns 0

    """

    conf = tomllib.load(open(conf_pth, "rb"))

    # Initialize streams as specified - first random streams
    sname, scfg = conf.get("random", {}).popitem()
    
    # Override config with provided parameters
    if n_channels is not None:
        scfg["n_channels"] = n_channels
    if msg_size is not None:
        scfg["msg_size"] = msg_size

    stream = MockupStream(name=sname, cfg=scfg, n_messages=n_messages)

    # collect stream names
    stream_name = stream.outlet.get_info().name()

    logger.debug(f"Initalized stream - {stream_name}")

    send_times = []
    try:

        default_data = np.ones((msg_size,n_channels), dtype=np.float64)
        assert stream.outlet.wait_for_consumers(20), "No consumers connected within 20s"

        time.sleep(1)

        # Warmup loop to stabilize the stream before measurements
        counter = 0
        for i in range(500):
            now = time.monotonic()
            stream.outlet.push_chunk(default_data*now)
            counter += 1
            # print(f"Warmup message {counter}/500")
        
        # time.sleep(0.01)

        for i in range(n_messages):
            if t_wait > 0:
                time.sleep(t_wait)  # Let Consumer process last message
            now = time.monotonic()
            stream.outlet.push_chunk(default_data * now)
            send_times.append(now)
            # print(f"Sent message {i+1}/{n_messages}")


    except KeyboardInterrupt:
        print("\n\nShutting down producer...")
    finally:
        print(f"Total messages sent: {len(send_times)}")

        send_times_diff = np.diff(send_times)
        mean_send_period = np.mean(send_times_diff)*1e6
        std_send_period = np.std(send_times_diff)*1e6
        max_send_period = np.max(send_times_diff)*1e6
        print(f"Average send period (us): {mean_send_period:.4f}")
        print(f"Max send period (us): {max_send_period:.4f}, idx: {np.argmax(send_times_diff)}")
        print(f"Std send period (us): {std_send_period:.4f}")

        results = pd.DataFrame({"Metric":["mean", "std", "max"],"Send_period": [mean_send_period, std_send_period, max_send_period]})
        results.to_csv(output_file, index=False)
        while stream.outlet.have_consumers() and not stop_event.is_set():
            print("Waiting to be closed...")
            time.sleep(1)
        
    return 0


def run_mockup_streamer_thread(
    random_data: bool = False,
    **kwargs,
) -> tuple[threading.Thread, threading.Event]:
    """Run the streaming within a separate thread and have a stop_event"""
    stop_event = threading.Event()

    thread = threading.Thread(
        target=run_stream,
        kwargs={"stop_event": stop_event, **kwargs},
    )

    thread.start()

    return thread, stop_event


if __name__ == "__main__":
    Fire(run_stream)
