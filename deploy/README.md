# Deribit L1 collector on AWS

Phase 0 (phase-0-edge-spike, section 2): `scripts/deribit_l1_collector.py` runs on a
small VM for 3-4 weeks and writes hourly parquet files; `scripts/load_l1.py` loads
the pulled files into the local ClickHouse.

Measured on 2026-09-24: 1078 BTC options + 14 futures (incl. BTC-PERPETUAL) + the
index = **1093 rows per minute**, ~47 KiB of parquet per minute, **~66 MiB per day,
~1.8 GiB over 4 weeks**.

## 1. Launch the instance

- Region **eu-west-2**, type **t4g.micro** (arm64) or t3.micro (x86).
- AMI: **Ubuntu 24.04 LTS** (arm64 for t4g).
- Storage: 20 GiB gp3 (data needs ~2 GiB; the rest is headroom).
- Security group: inbound **SSH 22 from your IP only**. Nothing else inbound; the
  collector only makes outbound connections to Deribit.
- Credit specification: leave the default (*unlimited* on t3/t4g).

## 2. Install

```bash
sudo apt-get update && sudo apt-get install -y git
sudo useradd --system --create-home --shell /usr/sbin/nologin zora
curl -LsSf https://astral.sh/uv/install.sh | sudo env UV_INSTALL_DIR=/usr/local/bin sh

sudo git clone https://github.com/DjokaIzPotoka/paun_research.git /opt/zora-research
sudo chown -R zora:zora /opt/zora-research

# Python 3.13 goes under /opt, not the zora home directory: the unit sets ProtectHome.
sudo -u zora env UV_PYTHON_INSTALL_DIR=/opt/zora-research/.python \
  UV_CACHE_DIR=/opt/zora-research/.uv-cache \
  uv sync --frozen --no-dev --project /opt/zora-research

sudo install -d -o zora -g zora -m 755 /var/lib/zora-l1
sudo cp /opt/zora-research/deploy/deribit-l1-collector.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now deribit-l1-collector
```

The repo is private, so the clone needs a deploy key or a token; copying a checkout
over with `scp -r` works too.

Check the clock is synchronised -- `snapshot_ts` is the VM's wall clock:

```bash
timedatectl status | grep synchronized   # System clock synchronized: yes
```

## 3. Check status

```bash
systemctl status deribit-l1-collector
journalctl -u deribit-l1-collector -f
journalctl -u deribit-l1-collector --since "1 hour ago" | grep -E "WARN|ERROR"
ls -l /var/lib/zora-l1/l1_snapshots/date=$(date -u +%F)/
```

Healthy is one line a minute like
`event=flush snapshot=16:47 rows=1093 gaps=0 waiting_meta=0 subscribed=1093`.
Things to look into: `rows` well below ~1090, `gaps` > 0 on consecutive minutes,
`waiting_meta` > 0, or `sink_write_failed`.

Also confirm CPU and memory on the VM itself -- measured locally at well under 1%
of a core and ~110 MB RSS, but that has not been checked on a micro instance:

```bash
top -b -n 1 -p "$(systemctl show -p MainPID --value deribit-l1-collector)"
```

## 4. Layout on the VM

```
/var/lib/zora-l1/
  l1_snapshots/date=YYYY-MM-DD/HH.parquet     sealed hour, never modified again
  l1_snapshots/date=YYYY-MM-DD/HH.late-*.parquet  rare rows for an already sealed hour
  l1_snapshots/.staging/...                   the current hour, minute parts
  collector_gaps/...                          same layout
  instruments/...                             every get_instruments response
  _state/last_flushed_ms
```

An hour is sealed (merged with an atomic rename) once it is over, so sealed files
are always complete and safe to copy while the collector runs.

## 5. Pull files and load them

From Linux, macOS or WSL (Git Bash has no rsync). Only sealed files are copied:

```bash
rsync -av --prune-empty-dirs \
  --exclude='.staging/' --exclude='_state/' --exclude='*.tmp' \
  --include='*/' --include='*.parquet' --exclude='*' \
  -e "ssh -i ~/.ssh/zora-l1.pem" \
  ubuntu@<VM_IP>:/var/lib/zora-l1/ /mnt/d/zora-research-data/l1/
```

Then, locally (ClickHouse from `zore-core/discovery/docker-compose.yml` must be up):

```bash
uv run python -m scripts.load_l1 --config scripts/load_l1.yaml
```

Re-running is safe: files already in `load_manifest` are skipped by sha256, and the
tables deduplicate on their key. Query with `FINAL`, e.g.
`SELECT count() FROM optrv.l1_snapshots FINAL`.

## 6. Stopping for good

The current hour lives in `.staging/` until it is over. When the collection ends:

```bash
sudo systemctl disable --now deribit-l1-collector
sudo -u zora /opt/zora-research/.venv/bin/python -m scripts.deribit_l1_collector \
  --config /opt/zora-research/scripts/deribit_l1_collector.yaml --seal-and-exit
```

then run the rsync once more.
