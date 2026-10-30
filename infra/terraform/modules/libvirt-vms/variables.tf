variable "name_prefix" {
  description = "Prefix for all libvirt resources (pool, network, domains)."
  type        = string
  default     = "rtdaas"
}

variable "node_count" {
  description = "Number of VMs. The first node becomes the k3s server, the rest agents."
  type        = number
  default     = 4
  validation {
    condition     = var.node_count >= 1
    error_message = "node_count must be at least 1."
  }
}

variable "memory_mb" {
  description = "RAM per VM in MiB."
  type        = number
  default     = 8192
}

variable "vcpu" {
  description = "vCPUs per VM."
  type        = number
  default     = 4
}

variable "disk_gb" {
  description = "Root disk size per VM in GiB (the cloud image is resized to this)."
  type        = number
  default     = 60
}

variable "base_image_url" {
  description = "Ubuntu 24.04 cloud image (qcow2). A local path works too."
  type        = string
  default     = "https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img"
}

variable "ssh_public_key" {
  description = "SSH public key installed for the `ubuntu` user."
  type        = string
}

variable "network_cidr" {
  description = "NAT network CIDR for the VMs."
  type        = string
  default     = "10.42.100.0/24"
}

variable "pool_path" {
  # Under images/ for Ubuntu's AppArmor: virt-aa-helper may only read backing files there, and
  # qemu gets "Permission denied" on the base image anywhere else.
  description = "Host directory backing the libvirt storage pool."
  type        = string
  default     = "/var/lib/libvirt/images/rtdaas"
}

variable "inventory_path" {
  description = "Where to write the rendered Ansible inventory."
  type        = string
  default     = "../../../ansible/inventory/hosts.ini"
}
