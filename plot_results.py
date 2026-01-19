import math
import matplotlib.pyplot as plt
import numpy as np
import os

results_folder = "results"
result_files = [f for f in os.listdir(results_folder) if "send_times" in f]

# Layout: 4 columns minimum one row; add rows as needed
n_plots = len(result_files)
cols = 4
rows = max(1, math.ceil(n_plots / cols))

fig, axes = plt.subplots(rows, cols, sharey=True,sharex=True, figsize=(4 * cols, 4 * rows))
axes = np.atleast_1d(axes).ravel()

legend_handles = None
legend_labels = None

for idx, file in enumerate(sorted(result_files)):
    ax1 = axes[idx]
    ax2 = ax1.twinx()

    freq, n_channels, msg_size, node, _ = file.split('_')
    print(f"Processing file: {file} | Freq: {freq} Hz | Channels: {n_channels} | Msg Size: {msg_size} ")

    receive_file = f"{freq}_{n_channels}_{msg_size}_recv_times.csv"
    recv_data = np.loadtxt(os.path.join(results_folder, receive_file), skiprows=1, delimiter=',')
    send_data = np.loadtxt(os.path.join(results_folder, file), skiprows=1, delimiter=',')

    ax2.plot(send_data[:, 1], label="Send_period", c="green")
    ax2.plot(recv_data[:, 2], label="Recv_period", c="red")
    ax1.plot(recv_data[:, 1], label="Latency", c="orange")


    # ax1.set_ylim(0, 1/float(freq))
    ax2.set_ylim(0, 1/float(freq)*5)
    ax1.set_title(f"Freq {freq} Hz | Ch {n_channels} | Msg {msg_size}")
    ax1.set_ylabel("Latency (s)")
    ax2.set_ylabel("Period (s)")
    ax1.set_xlabel("Message Index")

    # Capture legend entries once
    if legend_handles is None:
        h1, l1 = ax1.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        legend_handles = h1 + h2
        legend_labels = l1 + l2

# Hide any unused axes if plots < grid size
for j in range(n_plots, rows * cols):
    fig.delaxes(axes[j])

# Add legends once
if legend_handles:
    fig.legend(legend_handles, legend_labels, loc="upper center", ncol=3)

fig.tight_layout(rect=[0, 0, 1, 0.93])
plt.show()