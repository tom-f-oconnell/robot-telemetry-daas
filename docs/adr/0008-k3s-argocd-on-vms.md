# ADR 0008: k3s + Argo CD on Terraform-provisioned VMs (local libvirt or AWS EC2)

**Status:** accepted · **Date:** 2026-10-29

## Context

The platform should run as a multi-node system on a workstation (no cloud bill) and, with the
same manifests, on AWS. Managed Kubernetes (EKS) is the production answer but costs money to keep
up and is slow to create/destroy for a demo.

## Decision

Terraform provisions 4 Ubuntu VMs (libvirt locally, EC2 on AWS) and renders an Ansible
inventory. Ansible installs k3s (1 server, 3 agents) and Argo CD. Everything else — Strimzi,
Apicurio, spark-operator, Airflow, ECK, MinIO, Iceberg REST, kube-prometheus-stack and the
`rtdaas` workloads — is declared as Argo CD Applications (app-of-apps) and synced from git.
`deploy.yml` only bumps image tags in `kustomization.yaml`; Argo CD performs the rollout.

## Consequences

- The same `infra/k8s` directory targets both environments; EKS would need only a different
  Terraform module and storage classes.
- Secrets are templated (`stringData` placeholders); a real deployment should add
  sealed-secrets or External Secrets, noted in the runbook.
- Local VMs need ~32 GB RAM for the full stack; `docker compose` remains the single-host path.
