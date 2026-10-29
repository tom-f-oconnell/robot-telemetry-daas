output "node_ips" {
  value = module.cluster.node_ips
}

output "server_ip" {
  value = module.cluster.server_ip
}

output "bucket_raw" {
  value = module.cluster.bucket_raw
}

output "bucket_lake" {
  value = module.cluster.bucket_lake
}

output "ansible_inventory_path" {
  value = module.cluster.ansible_inventory_path
}

output "next_step" {
  value = "cd ../../../ansible && ansible-playbook -i inventory/hosts.ini site.yml"
}
