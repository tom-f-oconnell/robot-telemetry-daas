output "node_names" {
  description = "VM names; index 0 is the k3s server."
  value       = local.node_names
}

output "node_ips" {
  description = "Static IPs of the VMs, same order as node_names."
  value       = local.node_ips
}

output "server_ip" {
  description = "IP of the k3s server node."
  value       = local.node_ips[0]
}

output "ansible_inventory_path" {
  description = "Path of the rendered Ansible inventory."
  value       = local_file.ansible_inventory.filename
}

output "ansible_inventory" {
  description = "Rendered Ansible inventory content."
  value       = local_file.ansible_inventory.content
}
