# N Ubuntu 24.04 VMs on a local libvirt/KVM host, ready for Ansible + k3s.
#
# Host prerequisites: libvirtd, qemu-system-x86, genisoimage (cloud-init ISOs), xsltproc (the
# `xml` block below), and the user running terraform in the `libvirt` group. Provider connection
# defaults to qemu:///system (set in envs/local).

locals {
  node_names = [for i in range(var.node_count) : format("%s-node-%02d", var.name_prefix, i + 1)]
  # Static IPs: .10, .11, ... inside network_cidr
  node_ips = [for i in range(var.node_count) : cidrhost(var.network_cidr, 10 + i)]
}

resource "libvirt_pool" "this" {
  name = "${var.name_prefix}-pool"
  type = "dir"
  target {
    path = var.pool_path
  }
}

resource "libvirt_volume" "base" {
  name   = "${var.name_prefix}-ubuntu-noble-base.qcow2"
  pool   = libvirt_pool.this.name
  source = var.base_image_url
  format = "qcow2"
}

resource "libvirt_volume" "root" {
  count          = var.node_count
  name           = "${local.node_names[count.index]}-root.qcow2"
  pool           = libvirt_pool.this.name
  base_volume_id = libvirt_volume.base.id
  size           = var.disk_gb * 1024 * 1024 * 1024
  format         = "qcow2"
}

resource "libvirt_network" "this" {
  name      = "${var.name_prefix}-net"
  mode      = "nat"
  domain    = "${var.name_prefix}.local"
  addresses = [var.network_cidr]
  autostart = true
  dhcp {
    enabled = true
  }
  dns {
    enabled    = true
    local_only = true
  }
}

resource "libvirt_cloudinit_disk" "this" {
  count = var.node_count
  name  = "${local.node_names[count.index]}-cloudinit.iso"
  pool  = libvirt_pool.this.name

  user_data = templatefile("${path.module}/templates/cloud-init.yaml.tftpl", {
    hostname       = local.node_names[count.index]
    ssh_public_key = var.ssh_public_key
  })

  # The guest keeps DHCP: `network_interface.addresses` below reserves a fixed
  # lease per MAC in the libvirt network, which is what `wait_for_lease` relies on.
}

resource "libvirt_domain" "node" {
  count  = var.node_count
  name   = local.node_names[count.index]
  memory = var.memory_mb
  vcpu   = var.vcpu

  qemu_agent = true
  autostart  = true

  cloudinit = libvirt_cloudinit_disk.this[count.index].id

  cpu {
    mode = "host-passthrough"
  }

  network_interface {
    network_id     = libvirt_network.this.id
    hostname       = local.node_names[count.index]
    addresses      = [local.node_ips[count.index]]
    wait_for_lease = true
  }

  # `file`, not `volume_id`: with a pool-volume source Ubuntu's AppArmor helper does not follow
  # the backing chain and qemu gets "Permission denied" on the base image. A volume's id is its path.
  disk {
    file = libvirt_volume.root[count.index].id
  }

  xml {
    xslt = file("${path.module}/templates/qcow2-disk.xsl")
  }

  console {
    type        = "pty"
    target_port = "0"
    target_type = "serial"
  }

  graphics {
    type        = "vnc"
    listen_type = "address"
    autoport    = true
  }
}

resource "local_file" "ansible_inventory" {
  filename        = var.inventory_path
  file_permission = "0644"
  content = templatefile("${path.module}/templates/inventory.ini.tftpl", {
    server_name = local.node_names[0]
    server_ip   = local.node_ips[0]
    agents      = [for i in range(1, var.node_count) : { name = local.node_names[i], ip = local.node_ips[i] }]
    ssh_user    = "ubuntu"
  })
}
