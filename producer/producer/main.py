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
from dareplane_utils.general.time import sleep_s
import time
from fire import Fire
import base64

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
        self.sfreq = cfg["sampling_freq"]
        self.num_channels = cfg.get("n_channels", 1) if cfg["msg_size"] == 1 else 1
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
        if self.cfg["msg_size"] == 1:
            dtype = fmt_map.get(self.cfg.get("data_type", "float64"), "float64")
        else:
            dtype = fmt_map.get(self.cfg.get("data_type", "string"), "string")

        print(f"fs of mockstream: {self.sfreq}, dtype: {dtype}")

        info = pylsl.StreamInfo(
            self.cfg["stream_name"],
            self.cfg.get("stream_type", "EEG"),
            self.num_channels,
            self.sfreq,
            channel_format=dtype,
        )

        self.info = info
        self.outlet = pylsl.StreamOutlet(self.info)

    def push(self):

        n_required = (
            int((pylsl.local_clock() - self.t_start_s) * self.sfreq) - self.n_pushed
        )
        if n_required > 0:
            data = self.buffer[self.buffer_i : self.buffer_i + n_required]
            # if n_required > 1:
            #     logger.debug(f"Pushing {data.shape} samples for {self.name}, {data}")
            self.outlet.push_chunk(data)

            # if marker stream is associated -> push as well
            if self.outlet_mrk is not None:
                self.push_markers(
                    idx_from=self.n_pushed, idx_to=self.n_pushed + n_required
                )

            self.buffer_i += n_required
            self.n_pushed += n_required

            if self.buffer_i >= self.buffer.shape[0]:
                logger.debug("End of data reached, loading next data chunk")
                self.load_next_data()



def run_stream(
    stop_event: threading.Event = threading.Event(),
    conf_pth: Path = Path("./config/streams.toml"),
    n_messages: int = None,
    freq: float = None,
    n_channels: int = None,
    msg_size: int=None,
    output_file: str = "send_times.csv",
) -> int:
    """
    Parameters
    ----------
    stop_event : threading.Event
        event used to stop the streaming
    conf_pth : Path
        Path to config file
    n_samples : int, optional
        Number of samples to send (overrides config)
    freq : float, optional
        Sampling frequency in Hz (overrides config)
    n_channels : int, optional
        Number of channels (overrides config)

    Returns
    -------
    int
        returns 0

    """

    conf = tomllib.load(open(conf_pth, "rb"))

    # Initialize streams as specified - first random streams
    sname, scfg = conf.get("random", {}).popitem()
    
    # Override config with provided parameters
    if freq is not None:
        scfg["sampling_freq"] = freq
    if n_channels is not None:
        scfg["n_channels"] = n_channels
    if msg_size is not None:
        scfg["msg_size"] = msg_size


    stream = MockupStream(name=sname, cfg=scfg)

    # collect stream names
    stream_name = stream.outlet.get_info().name()

    logger.debug(f"Initalized stream - {stream_name}")

    # find fastest sampling rate
    dt = 1 / stream.sfreq

    send_delays = []
    send_times = []
    PERIOD_S = 1/stream.sfreq
    try:

        next_deadline = time.monotonic()
        default_data = np.ones((msg_size, n_channels), dtype=np.float64)
        assert stream.outlet.wait_for_consumers(5), "No consumers connected within 5s"

        for i in range(n_messages):
            while True:
                now = time.monotonic()
                if now >= next_deadline:
                    if msg_size == 1:
                        data = [now]*n_channels
                        stream.outlet.push_sample(data)
                    else:
                        data = default_data * now
                        payload = base64.b64encode(data.ravel().astype(np.float32).tobytes()).decode('ascii')
                        stream.outlet.push_sample([payload])
                    send_times.append(now)
                    # send_delays.append(np.float64(time.monotonic()) - now)
                    next_deadline += PERIOD_S
                    # print(f"Producer i: {i}, {data}")
                    break


    except KeyboardInterrupt:
        print("\n\nShutting down producer...")
    finally:
        print(f"Total messages sent: {len(send_times)}")
        # send_delays = send_delays[1:]
        # print(f"Average send time (s): {np.mean(send_delays)/1e9:.6f}")
        # print(f"Max send time (s): {np.max(send_delays)/1e9:.6f}, idx: {np.argmax(send_delays)}")
        # print(f"Std send time (s): {np.std(send_delays)/1e9:.6f}")
        # print('\n')

        send_times = send_times[1:]
        send_times_diff = np.diff(send_times)
        print(f"Average send period (s): {np.mean(send_times_diff):.6f}")
        print(f"Max send period (s): {np.max(send_times_diff):.6f}, idx: {np.argmax(send_times_diff)}")
        print(f"Std send period (s): {np.std(send_times_diff):.6f}")
        save = np.vstack((send_times, np.append(send_times_diff, 0)))
        np.savetxt(output_file, save.T, delimiter=',', header='send_time,send_period')
        while stream.outlet.have_consumers():
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
