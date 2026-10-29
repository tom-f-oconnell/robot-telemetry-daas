# Local multi-node environment: 1 k3s server + (node_count - 1) agents on libvirt/KVM.
#
#   cd infra/terraform/envs/local
#   cp terraform.tfvars.example terraform.tfvars   # paste your ssh public key
#   terraform init && terraform apply
#   cd ../../../ansible && ansible-playbook -i inventory/hosts.ini site.yml

module "vms" {
  source = "../../modules/libvirt-vms"

  name_prefix    = "rtdaas"
  node_count     = var.node_count
  memory_mb      = var.memory_mb
  vcpu           = var.vcpu
  disk_gb        = var.disk_gb
  base_image_url = var.base_image_url
  ssh_public_key = var.ssh_public_key
  network_cidr   = var.network_cidr
  inventory_path = "${path.module}/../../../ansible/inventory/hosts.ini"
}
