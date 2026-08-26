# Shipping MongoDB `DATA_SOURCE`

`DATA_SOURCE` is the **full local MongoDB data directory** (WiredTiger). You must ship the whole folder (or a tar of it), not individual `.wt` files.

`shipment.sh` stops Mongo (recommended), packs `DATA_SOURCE` into `shipments/DATA_SOURCE-<timestamp>.tar.gz`, uploads it over SSH, and optionally extracts it on the remote host.

## Requirements (source machine)

- `docker` / `docker compose`
- `tar`, `ssh`, `rsync`
- For password auth: `sshpass` (`sudo apt install sshpass`)
- SSH key auth is preferred

## Run

From the project root (where `docker-compose.yml` and `DATA_SOURCE` live):

```bash
chmod +x shipment.sh
./shipment.sh
```

The script asks for:


| Prompt               | Example                           |
| -------------------- | --------------------------------- |
| Remote IP / hostname | `203.0.113.10`                    |
| SSH port             | `22`                              |
| SSH username         | `ubuntu`                          |
| Auth                 | `1` = private key, `2` = password |
| Private key path     | `~/.ssh/id_ed25519`               |
| Remote directory     | `/var/www/{destination}`          |
| Extract on remote?   | `y` (default)                     |




## What gets created on the remote

If you choose extract (`y`):

```text
/var/www/DB/
├── DATA_SOURCE/                  # restored data dir
├── DATA_SOURCE.bak-<timestamp>/  # previous DATA_SOURCE, if any
└── DATA_SOURCE-<timestamp>.tar.gz
```



## Start Mongo on the destination

Use the **same major version** (`mongo:7`) and the **same** root credentials as the source `.env`.

Example (destination already has this repo / compose):

```bash
cd /var/www/SyncMongoDB
# ensure .env has the same LOCAL_MONGO_ROOT_USER / LOCAL_MONGO_ROOT_PASSWORD
docker compose up -d mongo
docker compose ps
```

Or a one-off container:

```bash
docker run -d --name mongo-restored \
  -p 127.0.0.1:27017:27017 \
  -v /var/www/SyncMongoDB/DATA_SOURCE:/data/db \
  -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD='your_secure_root_password' \
  mongo:7
```

> If `DATA_SOURCE` already contains a initialized database, Mongo uses the existing users/data; init env vars mainly matter on a first empty volume.



## Safety notes

- **Stop Mongo before copy** — the script offers this. Copying a live WiredTiger directory can corrupt the shipment.
- Ship the **entire** archive/directory — never a subset of `.wt` files.
- Match **MongoDB major version** on source and destination.
- Prefer **SSH keys**; passwords need `sshpass` and are less safe in shared shells.
- Archives under `shipments/` stay local unless you delete them when the script asks.



## Manual extract (if you skipped remote extract)

```bash
cd /var/www/SyncMongoDB
tar -xzf DATA_SOURCE-YYYYMMDD-HHMMSS.tar.gz
```



## Troubleshooting


| Symptom                         | What to check                                                  |
| ------------------------------- | -------------------------------------------------------------- |
| `Permission denied (publickey)` | Key path, `ssh-agent`, remote `authorized_keys`                |
| `sshpass: command not found`    | `sudo apt install sshpass` or use key auth                     |
| Remote Mongo won’t start        | Version mismatch, permissions on `DATA_SOURCE`, wrong password |
| Huge transfer time              | Expected for large DBs; rsync shows progress                   |


