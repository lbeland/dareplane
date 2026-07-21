# dareplane — IPC benchmark

Origin: https://github.com/bsdlab/Dareplane

[Dareplane](https://github.com/bsdlab/Dareplane) is a modular BCI framework
that communicates over [LSL](https://labstreaminglayer.org/) (Lab Streaming
Layer). Two benchmark variants are provided here:

- `benchmark.py` — pure-Python producer/consumer over LSL.
- `c_benchmark.py` — the same producer/consumer, implemented in C
  (`c-producer/`, `c-consumer/`) for a lower-overhead comparison.

## Install

From the repo root:

```bash
./bootstrap.sh --only dareplane            # venv + Python deps
./bootstrap.sh --only dareplane --with-builds --with-system-deps  # + c-producer/c-consumer binaries
```

## Run the benchmark

Python variant:

```bash
cd dareplane
venv/bin/python3 benchmark.py \
  --n_messages 1000 \
  --num_channels 3 \
  --msg_size 10 \
  --t_wait 0.0001 \
  --overwrite
```

C variant (requires `--with-builds` first):

```bash
cd dareplane
venv/bin/python3 c_benchmark.py \
  --n_messages 1000 \
  --num_channels 3 \
  --msg_size 10 \
  --t_wait 0.0001 \
  --overwrite
```

- `--n_messages`: number of timed messages sent
- `--num_channels` / `--msg_size`: shape of each message
- `--t_wait`: seconds the producer sleeps between sends (0 = no wait)
- `--overwrite`: rerun even if results for this channel/size combination
  already exist

Python-variant results are written to
`rt_results/<num_channels>_<msg_size>_{Producer,Consumer}.csv`, C-variant
results to `rt_c_results/` in the same naming scheme.
