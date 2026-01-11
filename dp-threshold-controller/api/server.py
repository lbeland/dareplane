from dareplane_utils.default_server.server import DefaultServer
from fire import Fire

from threshold_controller.main import get_main_thread
from threshold_controller.utils.logging import logger


def main(port: int = 8080, ip: str = "127.0.0.1", loglevel: int = 10):
    logger.setLevel(loglevel)
    pcommand_map = {"START": get_main_thread}

    server = DefaultServer(
        port,
        ip=ip,
        pcommand_map=pcommand_map,
        name="threshold_controller_server",
    )

    # initialize to start the socket
    server.init_server()
    # start processing of the server
    server.start_listening()

    return 0


if __name__ == "__main__":
    Fire(main)
