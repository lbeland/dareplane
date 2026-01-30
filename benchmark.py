"""Spawn mockup streamer (producer) and threshold controller (consumer) directly via their mains, benchmark IPC."""
import argparse
import os
import signal
import time
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
PRODUCER_ROOT = REPO_ROOT / "producer"
CONSUMER_ROOT = REPO_ROOT / "consumer"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_messages", type=int, default=1000)
    parser.add_argument("--freq", type=int, default=10000)
    parser.add_argument("--msg_size", type=int, default=10)
    parser.add_argument("--num_channels", type=int, default=1000)
    parser.add_argument("--output_file", type=str, default="send_times.csv")
    args = parser.parse_args()

    os.makedirs("results", exist_ok=True)

    # 1) Start consumer first
    output_path = REPO_ROOT / "results" / f"{args.freq}_{args.num_channels}_{args.msg_size}_recv_times.csv"
    # if os.path.exists(output_path):
    #     return  # Skip if already exists
    consumer = subprocess.Popen(
        [
            "python3",
            "-m",
            "consumer.main",
            f"--n_messages={args.n_messages}",
            f"--n_channels={args.num_channels}",
            f"--msg_size={args.msg_size}",
            f"--output_file={output_path}",
        ],
        cwd=str(CONSUMER_ROOT),
        preexec_fn=os.setsid,
    )
    # Give consumer time to bind to the LSL stream and prepare
    time.sleep(2)

    # 2) Start producer with custom parameters via Fire CLI
    output_path = REPO_ROOT / "results" / f"{args.freq}_{args.num_channels}_{args.msg_size}_send_times.csv"
    producer = subprocess.Popen(
        [
            "python3",
            "-m",
            "producer.main",
            f"--n_messages={args.n_messages}",
            f"--freq={args.freq}",
            f"--n_channels={args.num_channels}",
            f"--msg_size={args.msg_size}",
            f"--output_file={output_path}",
        ],
        cwd=str(PRODUCER_ROOT),
        preexec_fn=os.setsid,
    )
    try:
        # 3) Wait for completion
        consumer.wait(timeout=args.n_messages / args.freq + 10)
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Error during benchmark: {e}")
    finally:
        terminate(consumer)
        terminate(producer)
        print(f"Benchmark complete")


def terminate(proc):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except Exception:
        pass


if __name__ == "__main__":
    main()
