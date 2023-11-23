# Azure File Backup

## Prerequisites

### Install Python Virtual Environment and Systemd Development Packages

#### Centos/RHEL Based Distritbutions

```bash
yum install -y gcc python3-devel python-virtualenv systemd-devel
```

#### Debian/Ubuntu Based Distritbutions

```bash
apt install -y gcc libsystemd-dev pkg-config python3-dev python3-virtualenv
```

### Activate Python Virtual Environment and Install Dependencies

Build a virtual work environment for Python and install the [requirements](requirements.txt).

```bash
cd <repo content directory>
virtualenv -p python3 venv
source venv/bin/activate
pip install -r requirements.txt
```

### Configure Systemd

```bash
ln -s <repo content directory>/az-file-backup/az-file-backup.service /etc/systemd/system/az-file-backup.service
ln -s <repo content directory>/az-file-backup/az-file-backup.timer /etc/systemd/system/az-file-backup.timer
systemctl daemon-reload
systemctl enable --now az-file-backup.timer
```
