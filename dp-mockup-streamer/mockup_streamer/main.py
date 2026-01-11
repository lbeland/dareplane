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

import mne
import numpy as np
import pylsl
import pyxdf
from dareplane_utils.general.time import sleep_s
import time
from fire import Fire

from mockup_streamer.utils.logging import logger


class NoFilesFound(FileExistsError):
    pass


class EndOfDataError(KeyError):
    pass


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
        self.outlet_mrk = None  # will be populated once data is loaded and contains markers or if specified for random data  # noqa
        self.sfreq = cfg["sampling_freq"]
        self.n_channels = 0  # will be set once data is loaded

        self.load_next_data()

        # LSL - init after first data is loaded, as sfreq might be derived
        self.init_outlet()

        # after loading, we know if there is markers in the file, if yes
        # create a marker stream
        if self.markers is not None:
            default_name = self.cfg["stream_name"] + "_markers"
            self.init_outlet_mrk(
                self.cfg.get("markers", {}).get("marker_stream_name", default_name)
            )

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
        dtype = fmt_map.get(str(self.buffer.dtype), "string")

        print(f"fs of mockstream: {self.sfreq}")

        info = pylsl.StreamInfo(
            self.cfg["stream_name"],
            self.cfg.get("stream_type", "EEG"),
            self.n_channels,
            self.sfreq,
            channel_format=dtype,
        )

        self.info = info
        self.outlet = pylsl.StreamOutlet(self.info)

    def init_outlet_mrk(self, name: str):
        info = pylsl.StreamInfo(
            name,
            "Markers",
            1,
            pylsl.IRREGULAR_RATE,
            channel_format="string",
        )
        self.outlet_mrk = pylsl.StreamOutlet(info)

    def load_next_data(self):
        """
        Load the next data chunk into the buffer.

        This method handles loading data from files or generating random data
        based on the configuration. It initializes the buffer with the new data
        and resets the buffer index. If the configuration specifies markers,
        it also generates markers accordingly.

        If no files are provided, random data is generated. If files are provided,
        the next file in the list is loaded. If the end of the file list is reached
        and the mode is set to 'repeat', the file index is reset to the beginning.
        """

        # Have this as a warning since loading will likely take a substantial
        # amount of time
        logger.warning(f"Loading new data for {self.name}")

        # random
        if self.files == []:
            self.n_channels = self.cfg["n_channels"]

            logger.debug("Loading new random data")
            data = np.random.randn(
                self.sfreq * self.cfg.get("pre_buffer_s", 300),
                self.n_channels,
            )

            markers = None
            if self.cfg.get("markers", False):
                dt = self.cfg["markers"]["t_interval_s"]
                nmrk = int(self.cfg.get("pre_buffer_s", 300) / dt)
                # get markers as n_times x 2 (time, marker)
                markers = np.tile(
                    np.arange(nmrk, dtype="object") * dt * self.sfreq, (2, 1)
                ).T
                mval = self.cfg["markers"].get("values", "a")
                mval = mval if isinstance(mval, list) else [mval]
                mvals = np.tile(mval, nmrk // len(mval) + 1)
                markers[:, 1] = mvals[:nmrk]

        else:
            fl = self.files[self.file_i]
            data, markers, sfreq = load_data(fl, self.cfg)

            self.n_channels = data.shape[1]

            # Here could be an assert statement to check if an existing outlet
            # matches number of channels

            if self.sfreq == "derive":
                self.sfreq = sfreq

            if self.sfreq != sfreq:
                logger.warning(
                    f"Specified sampling rate {self.sfreq}Hz does not match"
                    f" sampling rate of file {fl} - {sfreq=}. Mockup stream"
                    " will provide output according to specified sampling rate"
                    " leading to a faster or slower replay."
                )

            self.file_i += 1

            if self.file_i >= len(self.files) and self.cfg.get("mode", "") == "repeat":
                self.file_i = 0

        # put data to buffer and start indexing from zero
        self.init_buffer(data, markers=markers)

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

    def push_markers(self, idx_from: int, idx_to: int):
        """Check if there is a marker within the index range and push if yes"""
        msk = (self.markers[:, 0] >= idx_from) & (self.markers[:, 0] < idx_to)
        logger.debug(f"Pushing {msk.sum()}, {idx_from=}, {idx_to=} markers")
        if msk.any():
            for mrk in self.markers[msk, 1]:
                self.outlet_mrk.push_sample([mrk])


def load_data(fp: Path, cfg: dict) -> tuple[np.ndarray, np.ndarray | None]:

    # load depending on suffix
    loaders = {
        ".vhdr": load_bv,
        ".fif": load_mne,
        ".xdf": load_xdf,
    }

    data, markers, sfreq = loaders[fp.suffix](fp, cfg)

    markers = None if len(markers) == 0 else markers

    return data, markers, sfreq


def load_bv(fp: Path, cfg: dict) -> tuple[np.ndarray, np.ndarray, float]:
    """Load for brainvision files"""
    raw = mne.io.read_raw_brainvision(fp, preload=True)

    data, markers = mne_raw_to_data_and_markers(raw, cfg)

    return data, markers, raw.info["sfreq"]


def load_mne(fp: Path, cfg: dict) -> tuple[np.ndarray, np.ndarray, float]:
    """Load for mne/fif files"""
    raw = mne.io.read_raw(fp, preload=True)

    data, markers = mne_raw_to_data_and_markers(raw, cfg)

    return data, markers, raw.info["sfreq"]


def load_xdf(fp: Path, cfg: dict) -> tuple[np.ndarray, np.ndarray, float]:
    """Load from xdf file - requires some configuration for which stream to
    use

    Parameters
    ----------
    fp : Path
        path to xdf file

    cfg : dict
        configuration dictionary specifying which stream to use and potential
        marker stream. Key value pairs are as follows:

        stream_name : str
            name of the stream to use

        marker_stream_name : str (optional)
            if given, look for stream and use to create markers. Markers are
            mapped to the nearest sample time point of the stream defined by
            `stream_name`

        pyxdf_kwargs : dict
            potential kwargs for pyxdf.load


    Returns
    -------
    tuple[np.ndarray, np.ndarray, float]
    """

    # print("Loading xdf")
    marker_stream = cfg.get("markers", {}).get("marker_stream_name", "")

    d = pyxdf.load_xdf(fp, **cfg.get("pyxdf_kwargs", {}))
    snames = [s["info"]["name"][0] for s in d[0]]

    try:
        sdata = d[0][snames.index(cfg["stream_name"])]
    except ValueError:
        raise KeyError(f"Stream {cfg['stream_name']=} not found in {snames=}")

    sfreq = float(sdata["info"]["nominal_srate"][0])
    data = sdata["time_series"]
    # print(f">>> {marker_stream=}")

    if marker_stream != "":
        # align the markers to match index of timepoints closest matching
        # do adjustments in loop as fully outer vectorized calculation
        # most likely is overkill (assuming n_markers << n_samples)
        td = sdata["time_stamps"]
        mdata = d[0][snames.index(marker_stream)]
        tm = mdata["time_stamps"]
        idx = [np.argmin(np.abs(td - t)) for t in tm]
        markers = np.asarray(
            [idx, [v[0] for v in mdata["time_series"]]], dtype="object"
        ).T

        # print(f"{markers=}")
    else:
        markers = np.asarray([])

    if isinstance(data, list):
        data = np.array(data)

    return data, markers, sfreq


def mne_raw_to_data_and_markers(
    raw: mne.io.BaseRaw, cfg: dict
) -> tuple[np.ndarray, np.ndarray]:

    # Limit to selected channels
    if cfg.get("select_channels", "") != "":
        raw.pick_channels(cfg["select_channels"])

    # Use the keys in a marker array
    ev, evid = mne.events_from_annotations(raw, verbose=False)
    imap = {v: k for k, v in evid.items()}
    ev = ev.astype("object")
    ev[:, 2] = [imap[i] for i in ev[:, 2]]

    data = raw.get_data().T

    return data, ev[:, [0, 2]]


# # IGNORE the meta data for now
# def add_bv_ch_info(info: pylsl.StreamInfo, raw: mne.io.BaseRaw):
#     """Add channel meta data to a pylsl.StreamInfo object
#
#     Parameters
#     ----------
#     info : pylsl.StreamInfo
#         info object to add the channel info to
#
#     raw : mne.io.BaseRaw
#         mne raw object to derive the channel names from
#
#     """
#     info.desc().append_child_value("manufacturer", "MockupStream")
#     chns = info.desc().append_child("channels")
#
#     for chan_ix, channel in enumerate(raw.info["chs"]):
#         ch = chns.append_child("channel")
#         ch.append_child_value("label", channel["ch_name"])
#         ch.append_child_value("unit", str(channel["range"]))
#         ch.append_child_value("type", channel["kind"]._name)
#         ch.append_child_value("scaling_factor", "1")
#         loc = ch.append_child("location")
#
#         if not np.isnan(channel["loc"][:3]).all():
#             for name, pos in zip(["X", "Y", "Z"], channel["loc"][:3]):
#                 loc.append_child_value(name, float(pos))
#
#
# def add_channel_info(info: pylsl.StreamInfo, conf: dict, data: Any):
#     """Add channel info depending on what type of data was provided"""
#
#     info_add_funcs = {
#         "BV": add_bv_ch_info,
#         "mne": add_bv_ch_info,
#         "random": add_bv_ch_info,  # random did load to mne.io.RawArray -> the meta data is comparable to the BV load. # noqa
#     }
#
#     info_add_funcs[conf["sources"]["source_type"]](info, data)


def get_data_and_channel_names(
    conf: dict,
) -> tuple[list[mne.io.BaseRaw], list[str]]:
    # Prepare data and stream outlets
    data = load_data(**conf["sources"])

    # infer available channels from data[0] --> assume number of channels stay constant. Working with set intersections did shuffle names to much and nested list comprehensions would be ugly here.        # noqa
    ch_names = (
        data[0].ch_names
        if conf["streaming"]["channel_name"] == "all"
        else conf["streaming"]["channel_name"]
    )

    data = [r.pick_channels(ch_names) for r in data]

    return data, ch_names


def glob_path_to_path_list(cfg: dict) -> list[Path]:

    fp = cfg.get("file_path", "")
    if isinstance(fp, list):
        return [Path(f) for f in fp]
    else:
        if fp == "":
            files = []
        else:
            fp = Path(fp)
            sep = "\\" if isinstance(fp, PureWindowsPath) else "/"

            # single file -> cast to list
            if "*" not in str(fp):
                files = [fp]
            else:  # glob

                # index of where the glob starts
                idx = ["*" in s for s in str(fp).split(sep)].index(True)

                files = list(
                    Path(f"{sep}".join(fp.parts[: idx - 1])).rglob(
                        f"{sep}".join(fp.parts[idx - 1 :])
                    )
                )

    return files


def run_stream(
    stop_event: threading.Event = threading.Event(),
    conf_pth: Path = Path("./config/streams.toml"),
) -> int:
    """
    Parameters
    ----------
    stop_event : threading.Event
        event used to stop the streaming

    conf : dict
        Configuration dictionary. If empty, will load the config specified at
        CONF_PATH (`./configs/streaming.yaml`).

    Returns
    -------
    int
        returns 0

    """

    conf = tomllib.load(open(conf_pth, "rb"))

    # Initialize streams as specified - first random streams
    sname, scfg = conf.get("random", {}).popitem()
    stream = MockupStream(name=sname, cfg=scfg)

    # collect stream names
    stream_name = stream.outlet.get_info().name()

    logger.debug(f"Initalized stream - {stream_name}")

    # find fastest sampling rate
    dt = 1 / stream.sfreq

    send_delays = []
    send_times = []
    PERIOD_NS = 1_000_000_000 * dt  # in ns

    next_deadline = time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)
    n_samples = 1000

    for i in range(n_samples):
        while True:
            now = time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)
            if now >= next_deadline:
                # logger.debug(f"Sending sample {i+1}/{n_samples}")
                stream.outlet.push_sample([np.uint64(now)])
                send_times.append(now)
                send_delays.append(np.uint64(time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)) - now)
                next_deadline += PERIOD_NS
                if i==1:
                    sleep_s(3)
                break

    print(f"Total messages sent: {len(send_delays)}")
    send_delays = send_delays[1:]
    print(f"Average send time (s): {np.mean(send_delays)/1e9:.6f}")
    print(f"Max send time (s): {np.max(send_delays)/1e9:.6f}, idx: {np.argmax(send_delays)}")
    print(f"Std send time (s): {np.std(send_delays)/1e9:.6f}")
    print('\n')

    send_times = np.diff(send_times)
    print(f"Average send period (s): {np.mean(send_times)/1e9:.6f}")
    print(f"Max send period (s): {np.max(send_times)/1e9:.6f}, idx: {np.argmax(send_times)}")
    print(f"Std send period (s): {np.std(send_times)/1e9:.6f}")

    # while not stop_event.is_set():
    #     # push is only sending samples if required as tracked by the
    #     # internal sampling frequency
    #     stream.push()

    #     sleep_s(dt)
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
