from fire import Fire
import signal
import sys

from functools import partial
from mi_paradigm.main import run_mi_task, Paradigm
from mi_paradigm.utils.logging import logger

from dareplane_utils.default_server.server import DefaultServer


def main(port: int = 8080, ip: str = "127.0.0.1", loglevel: int = 30):
    pdm = Paradigm()

    logger.setLevel(loglevel)
    logger.debug("Paradigm created")

    # partial is used so taht the function call will use the pdm instance
    pcommand_map = {"RUN": partial(run_mi_task, pdm)}

    server = DefaultServer(
        port, ip=ip, pcommand_map=pcommand_map, name="mi_paradigm_control_server"
    )

    # initialize to start the socket
    server.init_server()
    
    logger.info(
        f"Server intialized, starting to listen for connections on: {ip=}, {port=}"
    )

    def signal_handler(sig, frame):
        logger.info("Server interrupted by user (Ctrl + C)")
        pdm.close_window()
        logger.info("Paradigm closed, server shutting down")
        sys.exit(0)

    # Register signal handler for Ctrl + C
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # start processing of the server
        server.start_listening()
    except KeyboardInterrupt:
        logger.info("Server interrupted by user (Ctrl + C)")
        pdm.close_window()
        logger.info("Paradigm closed, server shutting down")
        return 0

    return 0


if __name__ == "__main__":
    Fire(main)