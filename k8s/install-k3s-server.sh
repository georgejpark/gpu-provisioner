#!/usr/bin/env bash
# Run ONCE on the control-plane node (e.g. pve5 or a small VM). Prints the
# join token the workflow needs (K3S_TOKEN) and writes a kubeconfig.
set -euo pipefail
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server --disable traefik --write-kubeconfig-mode 644" sh -
echo
echo "K3S_URL=https://$(hostname -I | awk '{print $1}'):6443"
echo "K3S_TOKEN=$(cat /var/lib/rancher/k3s/server/node-token)"
echo "kubeconfig: /etc/rancher/k3s/k3s.yaml  (copy to ~/.kube/config on the machine running the worker)"
