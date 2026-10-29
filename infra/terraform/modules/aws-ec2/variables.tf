variable "name_prefix" {
  description = "Prefix / Name tag for all resources."
  type        = string
  default     = "rtdaas"
}

variable "node_count" {
  description = "Number of EC2 instances. First is the k3s server, the rest agents."
  type        = number
  default     = 4
}

variable "instance_type" {
  description = "EC2 instance type."
  type        = string
  default     = "t3.xlarge"
}

variable "root_volume_gb" {
  description = "Root EBS volume size (GiB)."
  type        = number
  default     = 80
}

variable "ssh_public_key" {
  description = "SSH public key for the `ubuntu` user."
  type        = string
}

variable "admin_cidr" {
  description = "CIDR allowed to reach SSH (22), HTTP/HTTPS (80/443) and the Kubernetes API (6443). Use your own IP/32."
  type        = string
}

variable "vpc_cidr" {
  description = "VPC CIDR."
  type        = string
  default     = "10.60.0.0/16"
}

variable "subnet_cidr" {
  description = "Public subnet CIDR."
  type        = string
  default     = "10.60.1.0/24"
}

variable "bucket_suffix" {
  description = "Suffix appended to bucket names for global uniqueness. Empty = random."
  type        = string
  default     = ""
}

variable "inventory_path" {
  description = "Where to write the rendered Ansible inventory."
  type        = string
  default     = "../../../ansible/inventory/hosts.ini"
}

variable "tags" {
  description = "Extra tags applied to every resource."
  type        = map(string)
  default     = {}
}
