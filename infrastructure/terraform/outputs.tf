output "eks_cluster_id" {
  description = "EKS cluster ID"
  value       = aws_eks_cluster.vehicle_analytics.id
}

output "eks_cluster_endpoint" {
  description = "EKS cluster endpoint"
  value       = aws_eks_cluster.vehicle_analytics.endpoint
}

output "eks_cluster_certificate_authority" {
  description = "EKS cluster certificate authority data"
  value       = aws_eks_cluster.vehicle_analytics.certificate_authority[0].data
  sensitive   = true
}

output "eks_cluster_name" {
  description = "EKS cluster name"
  value       = aws_eks_cluster.vehicle_analytics.name
}

output "eks_node_group_id" {
  description = "EKS node group ID"
  value       = aws_eks_node_group.analytics_nodes.id
}

output "kafka_bootstrap_servers" {
  description = "Kafka bootstrap servers"
  value       = "${helm_release.kafka.name}-kafka.${kubernetes_namespace.vehicle_analytics.metadata[0].name}.svc.cluster.local:9092"
}

output "grafana_endpoint" {
  description = "Grafana dashboard endpoint"
  value       = "http://${helm_release.prometheus_stack.name}-grafana.${helm_release.prometheus_stack.namespace}.svc.cluster.local"
}

output "spark_master_endpoint" {
  description = "Spark master endpoint"
  value       = "spark://${helm_release.spark_operator.name}-spark-operator-spark-master.${helm_release.spark_operator.namespace}.svc.cluster.local:7077"
}

output "kubectl_config" {
  description = "Command to configure kubectl"
  value       = "aws eks update-kubeconfig --name ${aws_eks_cluster.vehicle_analytics.name} --region ${var.aws_region}"
}

output "namespace" {
  description = "Kubernetes namespace for vehicle analytics"
  value       = kubernetes_namespace.vehicle_analytics.metadata[0].name
}