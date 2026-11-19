#!/usr/bin/env bash
# gateway.sh: sets up the gateway box (Ubuntu, public IP, run as root): DuckDNS, tailscale + peer relay, the smg router
#   service and Caddy. Needs inbound TCP 80 and 443 (certificate checks, HTTPS), UDP 41641 (direct tailnet paths) and
#   UDP 40000 (peer relay) open. Rerun it to apply a new token or pick up a newer smg.
#   e.g.  sudo bash gateway/gateway.sh
set -euo pipefail

# 1. Read the repo's one .env, one folder up from this script (GATEWAY_HOST, GATEWAY_TOKEN, TS_AUTHKEY,
#    DNS_UPDATE_URL). Exported, so envsubst in step 6 sees the two values it fills in.
set -a; source "$(dirname "$0")/../.env"; set +a

# 2. Keep apt from restarting services on its own, before the first apt run below. needrestart ends every apt run,
#    the nightly security upgrade included, and by default restarts each service still running an old copy of an
#    updated library: here the router, killing every request in flight. List-only, it names them and restarts none.
#    Security updates still install every night; a patched library takes effect at the next restart you choose.
printf '%s\n' "\$nrconf{restart} = 'l';" > /etc/needrestart/conf.d/50-list-only.conf

# 3. Join the tailnet as shame-router (tailscale as a system service, WireGuard on UDP 41641) and be its peer relay
#    (UDP 40000). No name takeover: nothing uses this box's tailnet name. MagicDNS
#    stays on (tailscale up's default), so the router reaches sidecars by their tailnet names.
curl -fsSL https://tailscale.com/install.sh | sh
tailscale up --auth-key=file:/dev/stdin --hostname=shame-router <<<"$TS_AUTHKEY"
tailscale set --relay-server-port=40000

# 4. Run the router: shame-router.service (next to this script) as a systemd service; a rerun picks up a newer smg.
curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 sh
install -m 644 "$(dirname "$0")/shame-router.service" /etc/systemd/system/
systemctl daemon-reload
systemctl enable shame-router
systemctl restart shame-router
until curl -fs -o /dev/null http://127.0.0.1:30000/health; do sleep 2; done

# 5. Point GATEWAY_HOST at this box (DuckDNS points the name at the caller's IP) and wait until it resolves here, so
#    Let's Encrypt's check in step 6 reaches it. Public names still resolve through the cloud's resolver with tailscale
#    up: it only takes the tailnet's own domain.
curl -fsS "$DNS_UPDATE_URL" > /dev/null
IP=$(curl -fsS https://checkip.amazonaws.com)
until [ "$(getent ahostsv4 "$GATEWAY_HOST" | awk 'NR == 1 {print $1}')" = "$IP" ]; do
  echo "waiting for $GATEWAY_HOST to resolve to $IP"; sleep 5
done

# 6. Put Caddy in front: https://GATEWAY_HOST/<rest> goes to the router for callers sending the token. Caddy comes from
#    its own apt repository and gets the certificate itself. gateway/Caddyfile (next to this
#    script) gets its two placeholders filled, both exported by step 1, into a file only root and Caddy can read: the
#    package's own Caddyfile is world-readable, so it's locked before the token goes in. 403 without the token means
#    HTTPS is up.
curl -fsSL https://dl.cloudsmith.io/public/caddy/stable/gpg.key |
  gpg --yes --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -fsSL https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt > /etc/apt/sources.list.d/caddy-stable.list
apt-get update -q && DEBIAN_FRONTEND=noninteractive apt-get install -yq caddy gettext-base
install -m 640 -g caddy /dev/null /etc/caddy/Caddyfile
envsubst '$GATEWAY_HOST $GATEWAY_TOKEN' < "$(dirname "$0")/Caddyfile" > /etc/caddy/Caddyfile
systemctl restart caddy
until [ "$(curl -s -o /dev/null -w '%{http_code}' "https://$GATEWAY_HOST/")" = 403 ]; do sleep 2; done

# 7. Check the whole path once, with the token, through Caddy to the router: its /health answers 200 with no workers
#    yet (/v1/models answers 503 until a sidecar joins). The token goes in on stdin, as in step 3, so it's never on a
#    command line. Then print the endpoint for clients; sidecars find it through GATEWAY_HOST in the same .env.
curl -fsS -o /dev/null -H @- "https://$GATEWAY_HOST/health" <<<"Authorization: Bearer $GATEWAY_TOKEN"
echo "shame router: https://$GATEWAY_HOST/v1, key: GATEWAY_TOKEN from .env"
