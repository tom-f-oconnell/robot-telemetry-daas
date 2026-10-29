output "node_names" {
  value = local.node_names
}

output "node_ips" {
  description = "Public IPs, same order as node_names (index 0 = k3s server)."
  value       = aws_instance.node[*].public_ip
}

output "node_private_ips" {
  value = aws_instance.node[*].private_ip
}

output "server_ip" {
  value = aws_instance.node[0].public_ip
}

output "bucket_raw" {
  value = aws_s3_bucket.raw.bucket
}

output "bucket_lake" {
  value = aws_s3_bucket.lake.bucket
}

output "ansible_inventory_path" {
  value = local_file.ansible_inventory.filename
}

output "ansible_inventory" {
  value = local_file.ansible_inventory.content
}
