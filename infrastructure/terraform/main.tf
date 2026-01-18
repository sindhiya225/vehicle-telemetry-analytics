terraform {
  required_version = ">= 1.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

provider "kubernetes" {
  host                   = data.aws_eks_cluster.cluster.endpoint
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.cluster.certificate_authority.0.data)
  token                  = data.aws_eks_cluster_auth.cluster.token
}

provider "helm" {
  kubernetes {
    host                   = data.aws_eks_cluster.cluster.endpoint
    cluster_ca_certificate = base64decode(data.aws_eks_cluster.cluster.certificate_authority.0.data)
    token                  = data.aws_eks_cluster_auth.cluster.token
  }
}

# EKS Cluster
resource "aws_eks_cluster" "vehicle_analytics" {
  name     = var.cluster_name
  role_arn = aws_iam_role.eks_cluster.arn
  version  = var.kubernetes_version

  vpc_config {
    subnet_ids              = aws_subnet.eks_subnets[*].id
    endpoint_private_access = true
    endpoint_public_access  = true
    security_group_ids      = [aws_security_group.eks_cluster.id]
  }

  enabled_cluster_log_types = ["api", "audit", "authenticator", "controllerManager", "scheduler"]

  tags = {
    Environment = var.environment
    Project     = "Vehicle-Analytics"
  }
}

# EKS Node Group
resource "aws_eks_node_group" "analytics_nodes" {
  cluster_name    = aws_eks_cluster.vehicle_analytics.name
  node_group_name = "analytics-node-group"
  node_role_arn   = aws_iam_role.eks_nodes.arn
  subnet_ids      = aws_subnet.eks_subnets[*].id

  scaling_config {
    desired_size = var.node_desired_size
    max_size     = var.node_max_size
    min_size     = var.node_min_size
  }

  instance_types = var.node_instance_types

  remote_access {
    ec2_ssh_key               = var.ssh_key_name
    source_security_group_ids = [aws_security_group.eks_nodes.id]
  }

  update_config {
    max_unavailable = 1
  }

  tags = {
    Environment = var.environment
    Project     = "Vehicle-Analytics"
  }
}

# Kubernetes Namespace
resource "kubernetes_namespace" "vehicle_analytics" {
  metadata {
    name = "vehicle-analytics"
    labels = {
      name = "vehicle-analytics"
      environment = var.environment
    }
  }
}

# Storage Class for Kafka
resource "kubernetes_storage_class" "kafka_storage" {
  metadata {
    name = "kafka-ssd"
  }

  storage_provisioner = "kubernetes.io/aws-ebs"
  reclaim_policy      = "Retain"
  volume_binding_mode = "WaitForFirstConsumer"

  parameters = {
    type      = "gp3"
    iops      = "3000"
    throughput = "125"
    encrypted = "true"
  }

  allow_volume_expansion = true
}

# Spark Operator via Helm
resource "helm_release" "spark_operator" {
  name       = "spark-operator"
  repository = "https://googlecloudplatform.github.io/spark-on-k8s-operator"
  chart      = "spark-operator"
  version    = "1.1.27"
  namespace  = "spark-operator"

  create_namespace = true

  set {
    name  = "serviceAccounts.spark.name"
    value = "spark"
  }

  set {
    name  = "image.tag"
    value = "v1beta2-1.3.8-3.1.1"
  }

  set {
    name  = "enableWebhook"
    value = "true"
  }
}

# Kafka via Helm
resource "helm_release" "kafka" {
  name       = "kafka"
  repository = "https://charts.bitnami.com/bitnami"
  chart      = "kafka"
  version    = "22.1.7"
  namespace  = kubernetes_namespace.vehicle_analytics.metadata[0].name

  values = [
    <<-EOT
    replicas: 3
    persistence:
      enabled: true
      storageClass: "${kubernetes_storage_class.kafka_storage.metadata[0].name}"
      size: 10Gi
    resources:
      requests:
        memory: 1Gi
        cpu: 500m
      limits:
        memory: 2Gi
        cpu: 1000m
    zookeeper:
      replicaCount: 3
      persistence:
        enabled: true
        storageClass: "${kubernetes_storage_class.kafka_storage.metadata[0].name}"
        size: 5Gi
    EOT
  ]

  depends_on = [
    aws_eks_node_group.analytics_nodes,
    kubernetes_storage_class.kafka_storage
  ]
}

# Prometheus Stack for Monitoring
resource "helm_release" "prometheus_stack" {
  name       = "prometheus-stack"
  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "kube-prometheus-stack"
  version    = "46.8.0"
  namespace  = "monitoring"

  create_namespace = true

  values = [
    <<-EOT
    grafana:
      enabled: true
      adminPassword: "${var.grafana_password}"
      persistence:
        enabled: true
        storageClassName: "${kubernetes_storage_class.kafka_storage.metadata[0].name}"
        size: 5Gi
      dashboardProviders:
        dashboardproviders.yaml:
          apiVersion: 1
          providers:
          - name: 'default'
            orgId: 1
            folder: ''
            type: file
            disableDeletion: false
            editable: true
            options:
              path: /var/lib/grafana/dashboards/default
      dashboards:
        default:
          kafka-dashboard:
            url: https://raw.githubusercontent.com/grafana/grafana/main/public/dashboards/12176.json
          spark-dashboard:
            gnetId: 11458
            datasource: Prometheus
      service:
        type: LoadBalancer
        port: 80
        targetPort: 3000

    prometheus:
      prometheusSpec:
        retention: 30d
        storageSpec:
          volumeClaimTemplate:
            spec:
              storageClassName: "${kubernetes_storage_class.kafka_storage.metadata[0].name}"
              accessModes: ["ReadWriteOnce"]
              resources:
                requests:
                  storage: 20Gi

    alertmanager:
      alertmanagerSpec:
        storage:
          volumeClaimTemplate:
            spec:
              storageClassName: "${kubernetes_storage_class.kafka_storage.metadata[0].name}"
              accessModes: ["ReadWriteOnce"]
              resources:
                requests:
                  storage: 5Gi
    EOT
  ]
}