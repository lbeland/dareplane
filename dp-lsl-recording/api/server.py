from dareplane_utils.default_server.server import DefaultServer
from fire import Fire
import signal
import sys

from lsl_recorder.main import init_lsl_recorder_com
from lsl_recorder.utils.logging import logger


def main(port: int = 8080, ip: str = "127.0.0.1", loglevel: int = 10):
    logger.setLevel(loglevel)

    # Get the communication manager
    lslm = init_lsl_recorder_com()



    pcommand_map = {
        "SELECT_ALL": lslm.select_all,
        "SET_SAVE_PATH": lslm.set_recording_file,
        "UPDATE": lslm.update,
        "RECORD": lslm.record,
        "STOPRECORD": lslm.stop,
    }

    server = DefaultServer(
        port, ip=ip, pcommand_map=pcommand_map, name="lsl_control_server"
    )

    # initialize to start the socket
    server.init_server()

    
    def signal_handler(sig, frame):
        logger.info("Server interrupted by user (Ctrl + C)")
        logger.info("Paradigm closed, server shutting down")
        sys.exit(0)

    # Register signal handler for Ctrl + C
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # start processing of the server
        server.start_listening()
    except KeyboardInterrupt:
        logger.info("Server interrupted by user (Ctrl + C)")
        logger.info("Paradigm closed, server shutting down")
        return 0

    return 0


if __name__ == "__main__":
    Fire(main)
