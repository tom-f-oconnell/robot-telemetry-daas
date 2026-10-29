# AWS environment: k3s on EC2 + S3 buckets. Destroy when done (`terraform destroy`);
# 4 × t3.xlarge is roughly $0.67/hour on-demand in us-west-2.
#
#   cd infra/terraform/envs/aws
#   cp terraform.tfvars.example terraform.tfvars
#   terraform init && terraform apply
#   cd ../../../ansible && ansible-playbook -i inventory/hosts.ini site.yml

module "cluster" {
  source = "../../modules/aws-ec2"

  name_prefix    = "rtdaas"
  node_count     = var.node_count
  instance_type  = var.instance_type
  root_volume_gb = var.root_volume_gb
  ssh_public_key = var.ssh_public_key
  admin_cidr     = var.admin_cidr
  bucket_suffix  = var.bucket_suffix
  inventory_path = "${path.module}/../../../ansible/inventory/hosts.ini"
}
