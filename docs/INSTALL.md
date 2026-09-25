# Installing Soele

Single-box setup first; multi-node and containers after.

## Requirements

- Linux box on your LAN (a small VM or LXC is plenty; the daemon is light)
- Python 3.11+
- tmux (terminal mode; chat mode works without it)
- Claude Code installed and logged in as the user the daemon runs as, or a
  long-lived token from `claude setup-token`

## Single box

```sh
git clone <this repo> /opt/soele
cd /opt/soele
python3 -m venv .venv
.venv/bin/pip install fastapi 'uvicorn[standard]' httpx
cp hub.env.example /etc/hub.env && chmod 600 /etc/hub.env
# edit /etc/hub.env: set CLAUDE_CODE_OAUTH_TOKEN
cp hub.service /etc/systemd/system/
systemctl enable --now hub.service
```

Open `http://<box>:8800`. New chat, pick a directory, go.

Sessions spawn as the service user in the directory you choose. Give the
daemon a user whose reach matches what you want sessions to touch; see
SECURITY.md for the folder-scoping pattern.

## Phone access

The UI is a PWA. On your LAN, open it in the phone browser and add to home
screen. For access away from home, do NOT port-forward :8800; put a reverse
proxy with TLS and auth in front (nginx + a cert, or a VPN like Tailscale and
skip the public exposure entirely). The service worker needs an HTTPS origin
for push notifications.

For a real launcher app on Android (no browser chrome), build the TWA APK
against your own origin: [ANDROID.md](ANDROID.md).

## Multi-node

Run the same daemon on each machine (`HUB_NODE=<name>` in each box's env).
Pick one as the gateway and list the rest in its unit as
`HUB_PEERS=name=http://host:8800,name2=...`. The gateway merges session lists
and proxies turns; each session lives and stays on its own node.

## Worker containers (Proxmox)

On a Proxmox host, set `HUB_CTS=1` and `HUB_CT_BASE_IP=<golden template's IP>`.
The daemon can then clone a golden LXC template per project, start/stop the
clones, and run sessions inside them via `pct exec`. Building the golden
template (Claude login, your dotfiles, network) is its own topic: see the
companion [golden-container](https://github.com/Floral-Overcast/golden-container) project.

## Usage meter (optional)

`proxy.py` is a small pass-through proxy that reads rate-limit headers off
Anthropic API responses and writes a snapshot the UI renders. Run it as a
second service (port 8811), then point Claude Code through it with
`ANTHROPIC_BASE_URL=http://127.0.0.1:8811` in the environment your sessions
spawn with. Without the proxy the meter simply stays hidden.
