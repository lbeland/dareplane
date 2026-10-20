"""Spawn mockup streamer (producer) and threshold controller (consumer) directly via their mains, benchmark IPC."""
import argparse
import os
import signal
import time
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
PRODUCER_ROOT = REPO_ROOT / "c-producer"
CONSUMER_ROOT = REPO_ROOT / "c-consumer"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_messages", type=int, default=1000)
    parser.add_argument("--msg_size", type=int, default=10)
    parser.add_argument("--num_channels", type=int, default=3)
    parser.add_argument("--max_buffer_size", type=int, default=10)  # not used
    parser.add_argument("--t_wait", type=float, default=0.0001)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--output_file", type=str, default="Producer.csv")
    args = parser.parse_args()

    results_folder = f"{REPO_ROOT}/rt_c_results"

    os.makedirs(results_folder, exist_ok=True)

    # 1) Start consumer first
    output_path = f"{results_folder}/{args.num_channels}_{args.msg_size}_Consumer.csv"
    if os.path.exists(output_path) and not args.overwrite:
        return

    consumer = subprocess.Popen(
        [
            "sudo", "chrt", "-f", "99",
            "taskset", "-c", "7",
            str(CONSUMER_ROOT / "bin" / "consumer.bin"),
            f"--n_messages={args.n_messages}",
            f"--msg_size={args.msg_size}",
            f"--output_file={output_path}",
        ],
        cwd=str(CONSUMER_ROOT),
        preexec_fn=os.setsid,
    )

    # 2) Start producer with custom parameters via Fire CLI
    output_path = f"{results_folder}/{args.num_channels}_{args.msg_size}_Producer.csv"
    producer = subprocess.Popen(
        [
            "sudo", "chrt", "-f", "99",
            "taskset", "-c", "6",
            str(PRODUCER_ROOT / "bin" / "producer.bin"),
            f"--n_messages={args.n_messages}",
            f"--num_channels={args.num_channels}",
            f"--msg_size={args.msg_size}",
            f"--t_wait={args.t_wait}",
            f"--output_file={output_path}",
        ],
        cwd=str(PRODUCER_ROOT),
        preexec_fn=os.setsid,
    )
    try:
        # 3) Wait for completion
        consumer.wait(30)
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
