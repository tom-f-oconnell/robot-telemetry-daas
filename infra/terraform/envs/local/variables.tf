variable "libvirt_uri" {
  description = "libvirt connection URI."
  type        = string
  default     = "qemu:///system"
}

variable "node_count" {
  type    = number
  default = 4
}

variable "memory_mb" {
  type    = number
  default = 12288
}

variable "vcpu" {
  type    = number
  default = 4
}

variable "disk_gb" {
  type    = number
  default = 60
}

variable "pool_path" {
  description = "Host directory for the VM disks (grows to tens of GB; a snapshotted ZFS pool keeps deleted data)."
  type        = string
  default     = "/var/lib/libvirt/images/rtdaas"
}

variable "ssh_public_key" {
  description = "Contents of ~/.ssh/id_ed25519.pub (or similar)."
  type        = string
}

variable "network_cidr" {
  type    = string
  default = "10.42.100.0/24"
}

variable "base_image_url" {
  type    = string
  default = "https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img"
}
