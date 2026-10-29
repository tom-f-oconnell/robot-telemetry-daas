variable "region" {
  type    = string
  default = "us-west-2"
}

variable "node_count" {
  type    = number
  default = 4
}

variable "instance_type" {
  type    = string
  default = "t3.xlarge"
}

variable "root_volume_gb" {
  type    = number
  default = 80
}

variable "ssh_public_key" {
  description = "Contents of your SSH public key."
  type        = string
}

variable "admin_cidr" {
  description = "Your public IP as a /32 (curl -s ifconfig.me) — the only source allowed on 22/80/443/6443."
  type        = string
}

variable "bucket_suffix" {
  description = "Optional fixed suffix for bucket names; random when empty."
  type        = string
  default     = ""
}
