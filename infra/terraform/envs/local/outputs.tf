output "node_ips" {
  value = module.vms.node_ips
}

output "server_ip" {
  value = module.vms.server_ip
}

output "ansible_inventory_path" {
  value = module.vms.ansible_inventory_path
}

output "next_step" {
  value = "cd ../../../ansible && ansible-playbook -i inventory/hosts.ini site.yml"
}
