# Ansible: k3s + Argo CD bootstrap

```bash
cd infra/ansible
ansible-galaxy collection install -r requirements.yml
ansible-playbook -i inventory/hosts.ini site.yml      # hosts.ini is written by terraform
export KUBECONFIG=$PWD/artifacts/kubeconfig.yaml
kubectl get nodes
kubectl get applications -n argocd
```

Plays, in order:

1. `common` — packages, sysctl (inotify, max_map_count, ip_forward), swap off, br_netfilter.
2. `k3s_server` — installs k3s `{{ k3s_version }}` with traefik disabled, waits for Ready,
   fetches kubeconfig + node token into `artifacts/`.
3. `k3s_agent` — joins the remaining hosts using the server's token.
4. `argocd` — helm, ingress-nginx, Argo CD (`infra/k8s/helm-values/argocd.yaml`), the
   `rtdaas-env-overlay` ConfigMap (S3 buckets/region for this environment), and the
   app-of-apps `infra/k8s/argocd/root.yaml`. From here on Argo CD reconciles the cluster.

Re-running the playbook is safe; every task is idempotent.
