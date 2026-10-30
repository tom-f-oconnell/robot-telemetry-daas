# Terraform environments

Two environments provision the same shape — one k3s server and N−1 agents plus
object storage — so the Ansible playbook and Argo CD manifests are identical
downstream.

| Env           | Provider                          | What you get                                                       |
|---------------|-----------------------------------|--------------------------------------------------------------------|
| `envs/local`  | `dmacvicar/libvirt` (KVM)         | 4 Ubuntu 24.04 VMs on a NAT network; MinIO runs in-cluster for S3  |
| `envs/aws`    | `hashicorp/aws`                   | 4 × t3.xlarge in a public subnet, 2 S3 buckets, scoped IAM role    |

Both write `infra/ansible/inventory/hosts.ini`.

## Local (libvirt)

Host prerequisites: `qemu-system-x86 libvirt-daemon-system virtinst genisoimage xsltproc`
(`qemu-kvm` is only a virtual package on Ubuntu 26.04), your user in the `libvirt` group,
~48 GB RAM free for the default 4 × 12 GB.

Disk: the full stack writes about 75 GB across the VM disks, more as telemetry accumulates. Set
`pool_path` in `terraform.tfvars` to a filesystem with room. Avoid a ZFS pool with automatic
snapshots (e.g. zfs-auto-snapshot): they pin everything the VMs write and delete, so the pool fills
much faster than the disks grow, and space only returns once the snapshots expire.

```bash
cd infra/terraform/envs/local
cp terraform.tfvars.example terraform.tfvars      # paste your SSH public key
terraform init
terraform apply                                   # ~3 min incl. image download
cd ../../../ansible
ansible-playbook -i inventory/hosts.ini site.yml  # k3s + Argo CD (~6 min)
export KUBECONFIG=$PWD/artifacts/kubeconfig.yaml
kubectl get applications -n argocd                # watch the platform come up
```

If the Ubuntu image download is slow, download it once and set
`base_image_url = "/var/lib/libvirt/images/noble-server-cloudimg-amd64.img"`.

## AWS

```bash
cd infra/terraform/envs/aws
cp terraform.tfvars.example terraform.tfvars      # ssh key + your /32
terraform init
terraform apply
cd ../../../ansible
ansible-playbook -i inventory/hosts.ini site.yml
```

The inventory carries `rtdaas_s3_bucket_raw`, `rtdaas_s3_bucket_lake` and
`rtdaas_aws_region`; the `argocd` role templates them into the `rtdaas-config`
ConfigMap overlay so Spark/Airflow use real S3 instead of MinIO. Remember
`terraform destroy` — the instances are the cost driver, the buckets are
`force_destroy = true`.

## Conventions

- Node 0 is always the k3s server; its IP is `output.server_ip`.
- State is local by default; an S3 backend block is sketched in `envs/aws/versions.tf`.
- Nothing here installs Kubernetes workloads — that is Ansible (k3s, Argo CD) and
  then Argo CD (everything else) from `infra/k8s/`.
