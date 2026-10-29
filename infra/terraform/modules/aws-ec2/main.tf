# Minimal AWS footprint for a k3s cluster + S3 buckets for the lake.
# Deliberately not EKS: a handful of EC2 instances keeps the bill small and the
# Ansible/k3s path identical to the libvirt environment.

locals {
  tags = merge({ Project = "rtdaas", ManagedBy = "terraform" }, var.tags)

  node_names    = [for i in range(var.node_count) : format("%s-node-%02d", var.name_prefix, i + 1)]
  bucket_suffix = var.bucket_suffix != "" ? var.bucket_suffix : random_id.suffix.hex
  bucket_raw    = "${var.name_prefix}-raw-${local.bucket_suffix}"
  bucket_lake   = "${var.name_prefix}-lake-${local.bucket_suffix}"
}

resource "random_id" "suffix" {
  byte_length = 3
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

# ----------------------------------------------------------------- network
resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = merge(local.tags, { Name = "${var.name_prefix}-vpc" })
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = merge(local.tags, { Name = "${var.name_prefix}-igw" })
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.this.id
  cidr_block              = var.subnet_cidr
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true
  tags                    = merge(local.tags, { Name = "${var.name_prefix}-public" })
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }
  tags = merge(local.tags, { Name = "${var.name_prefix}-public-rt" })
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "nodes" {
  name        = "${var.name_prefix}-nodes"
  description = "rtdaas k3s nodes"
  vpc_id      = aws_vpc.this.id
  tags        = merge(local.tags, { Name = "${var.name_prefix}-nodes" })

  ingress {
    description = "SSH from admin"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }
  ingress {
    description = "HTTP from admin"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }
  ingress {
    description = "HTTPS from admin"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }
  ingress {
    description = "Kubernetes API from admin"
    from_port   = 6443
    to_port     = 6443
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }
  ingress {
    description = "All traffic between nodes"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_key_pair" "this" {
  key_name   = "${var.name_prefix}-key"
  public_key = var.ssh_public_key
  tags       = local.tags
}

# ----------------------------------------------------------------- storage
resource "aws_s3_bucket" "raw" {
  bucket        = local.bucket_raw
  force_destroy = true
  tags          = merge(local.tags, { Name = local.bucket_raw, Tier = "raw" })
}

resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket_lake
  force_destroy = true
  tags          = merge(local.tags, { Name = local.bucket_lake, Tier = "lake" })
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ----------------------------------------------------------------- IAM (instance role scoped to the two buckets)
data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "s3" {
  statement {
    sid       = "ListBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"]
    resources = [aws_s3_bucket.raw.arn, aws_s3_bucket.lake.arn]
  }
  statement {
    sid = "ObjectRW"
    actions = [
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
      "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.raw.arn}/*", "${aws_s3_bucket.lake.arn}/*"]
  }
}

resource "aws_iam_role" "nodes" {
  name               = "${var.name_prefix}-nodes"
  assume_role_policy = data.aws_iam_policy_document.assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy" "s3" {
  name   = "${var.name_prefix}-s3"
  role   = aws_iam_role.nodes.id
  policy = data.aws_iam_policy_document.s3.json
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.nodes.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "nodes" {
  name = "${var.name_prefix}-nodes"
  role = aws_iam_role.nodes.name
}

# ----------------------------------------------------------------- compute
resource "aws_instance" "node" {
  count                       = var.node_count
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.nodes.id]
  key_name                    = aws_key_pair.this.key_name
  iam_instance_profile        = aws_iam_instance_profile.nodes.name
  associate_public_ip_address = true

  root_block_device {
    volume_size           = var.root_volume_gb
    volume_type           = "gp3"
    delete_on_termination = true
    encrypted             = true
  }

  metadata_options {
    http_tokens = "required"
  }

  user_data = templatefile("${path.module}/templates/user-data.sh.tftpl", {
    hostname = local.node_names[count.index]
  })

  tags = merge(local.tags, {
    Name = local.node_names[count.index]
    Role = count.index == 0 ? "k3s-server" : "k3s-agent"
  })
}

resource "local_file" "ansible_inventory" {
  filename        = var.inventory_path
  file_permission = "0644"
  content = templatefile("${path.module}/templates/inventory.ini.tftpl", {
    server_name = local.node_names[0]
    server_ip   = aws_instance.node[0].public_ip
    server_priv = aws_instance.node[0].private_ip
    agents = [for i in range(1, var.node_count) : {
      name = local.node_names[i]
      ip   = aws_instance.node[i].public_ip
      priv = aws_instance.node[i].private_ip
    }]
    ssh_user    = "ubuntu"
    bucket_raw  = local.bucket_raw
    bucket_lake = local.bucket_lake
    region      = data.aws_availability_zones.available.id
  })
}
