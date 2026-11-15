"""Provider service -> FOCUS ServiceName / ServiceCategory / ServiceSubcategory.

This is a curated, auditable table (published as ``focus.main.service_map``). Keys
are the provider's own service identifier as it appears in exports: the AWS
``line_item_product_code``, the Azure ``MeterCategory`` (or ``ServiceFamily``)
and the GCP ``service.description``. Unknown services map to ``Other`` /
``Other (Other)`` so ``ServiceCategory`` is never NULL; ``focus_validate`` and
``x_ServiceCode`` make those rows easy to find and add here.
"""

from __future__ import annotations

from typing import NamedTuple


class Service(NamedTuple):
    """One provider service and its FOCUS classification."""

    provider: str
    code: str
    name: str
    category: str
    subcategory: str


_AWS = [
    ("AmazonEC2", "Amazon Elastic Compute Cloud", "Compute", "Virtual Machines"),
    ("AWSLambda", "AWS Lambda", "Compute", "Serverless Compute"),
    ("ComputeSavingsPlans", "Savings Plans for AWS Compute usage", "Compute", "Other (Compute)"),
    ("AmazonECS", "Amazon Elastic Container Service", "Compute", "Containers"),
    ("AmazonEKS", "Amazon Elastic Kubernetes Service", "Compute", "Containers"),
    ("AmazonECR", "Amazon Elastic Container Registry", "Compute", "Containers"),
    ("AmazonLightsail", "Amazon Lightsail", "Compute", "Virtual Machines"),
    ("AmazonWorkSpaces", "Amazon WorkSpaces", "Compute", "End User Computing"),
    ("AmazonS3", "Amazon Simple Storage Service", "Storage", "Object Storage"),
    ("AmazonS3GlacierDeepArchive", "Amazon S3 Glacier Deep Archive", "Storage", "Backup Storage"),
    ("AmazonGlacier", "Amazon Glacier", "Storage", "Backup Storage"),
    ("AmazonEFS", "Amazon Elastic File System", "Storage", "File Storage"),
    ("AmazonFSx", "Amazon FSx", "Storage", "File Storage"),
    ("AWSBackup", "AWS Backup", "Storage", "Backup Storage"),
    ("AmazonRDS", "Amazon Relational Database Service", "Databases", "Relational Databases"),
    ("AmazonDynamoDB", "Amazon DynamoDB", "Databases", "NoSQL Databases"),
    ("AmazonElastiCache", "Amazon ElastiCache", "Databases", "Caching"),
    ("AmazonRedshift", "Amazon Redshift", "Databases", "Data Warehouses"),
    ("AmazonDocDB", "Amazon DocumentDB", "Databases", "NoSQL Databases"),
    ("AmazonTimestream", "Amazon Timestream", "Databases", "Time Series Databases"),
    ("AmazonVPC", "Amazon Virtual Private Cloud", "Networking", "Network Infrastructure"),
    ("AmazonCloudFront", "Amazon CloudFront", "Networking", "Content Delivery"),
    ("AmazonRoute53", "Amazon Route 53", "Networking", "Network Routing"),
    ("AWSELB", "Elastic Load Balancing", "Networking", "Application Networking"),
    ("AWSDirectConnect", "AWS Direct Connect", "Networking", "Network Connectivity"),
    ("AWSDataTransfer", "AWS Data Transfer", "Networking", "Network Connectivity"),
    ("AmazonApiGateway", "Amazon API Gateway", "Integration", "API Management"),
    ("AWSQueueService", "Amazon Simple Queue Service", "Integration", "Messaging"),
    ("AmazonSNS", "Amazon Simple Notification Service", "Integration", "Messaging"),
    ("AmazonMSK", "Amazon Managed Streaming for Apache Kafka", "Integration", "Messaging"),
    ("AWSStepFunctions", "AWS Step Functions", "Integration", "Workflow Orchestration"),
    ("AmazonAthena", "Amazon Athena", "Analytics", "Analytics Platforms"),
    ("AWSGlue", "AWS Glue", "Analytics", "Data Processing"),
    ("ElasticMapReduce", "Amazon EMR", "Analytics", "Data Processing"),
    ("AmazonKinesis", "Amazon Kinesis", "Analytics", "Streaming Analytics"),
    ("AmazonES", "Amazon OpenSearch Service", "Analytics", "Search"),
    ("AmazonQuickSight", "Amazon QuickSight", "Analytics", "Business Intelligence"),
    ("AmazonSageMaker", "Amazon SageMaker", "AI and Machine Learning", "Machine Learning"),
    ("AmazonBedrock", "Amazon Bedrock", "AI and Machine Learning", "Generative AI"),
    ("AmazonCloudWatch", "Amazon CloudWatch", "Management and Governance", "Observability"),
    ("AWSCloudTrail", "AWS CloudTrail", "Management and Governance", "Compliance"),
    ("AWSConfig", "AWS Config", "Management and Governance", "Compliance"),
    ("AWSCostExplorer", "AWS Cost Explorer", "Management and Governance", "Cost Management"),
    ("AWSSupportBusiness", "AWS Support (Business)", "Management and Governance", "Support"),
    ("AWSSupportEnterprise", "AWS Support (Enterprise)", "Management and Governance", "Support"),
    ("AWSSecretsManager", "AWS Secrets Manager", "Security", "Secret Management"),
    ("awskms", "AWS Key Management Service", "Security", "Secret Management"),
    ("AmazonGuardDuty", "Amazon GuardDuty", "Security", "Threat Detection and Response"),
    ("AWSSecurityHub", "AWS Security Hub", "Security", "Security Posture Management"),
    ("awswaf", "AWS WAF", "Networking", "Network Security"),
    ("AWSShield", "AWS Shield", "Networking", "Network Security"),
    ("AmazonCognito", "Amazon Cognito", "Identity", "Identity and Access Management"),
    ("AWSIoT", "AWS IoT Core", "Internet of Things", "IoT Platforms"),
    ("AWSCodeBuild", "AWS CodeBuild", "Developer Tools", "Continuous Integration and Deployment"),
    ("AWSDatabaseMigrationSvc", "AWS Database Migration Service", "Migration", "Data Migration"),
    ("AmazonSES", "Amazon Simple Email Service", "Business Applications", "Productivity and Collaboration"),
    ("AWSAmplify", "AWS Amplify", "Web", "Application Platforms"),
]

_AZURE = [
    ("Virtual Machines", "Virtual Machines", "Compute", "Virtual Machines"),
    ("Virtual Machines Licenses", "Virtual Machines Licenses", "Compute", "Virtual Machines"),
    ("Azure Kubernetes Service", "Azure Kubernetes Service", "Compute", "Containers"),
    ("Container Instances", "Container Instances", "Compute", "Containers"),
    ("Container Registry", "Container Registry", "Compute", "Containers"),
    ("Functions", "Azure Functions", "Compute", "Serverless Compute"),
    ("Azure App Service", "Azure App Service", "Web", "Application Platforms"),
    ("Azure Virtual Desktop", "Azure Virtual Desktop", "Compute", "End User Computing"),
    ("Storage", "Storage", "Storage", "Storage Platforms"),
    ("Backup", "Azure Backup", "Storage", "Backup Storage"),
    ("SQL Database", "Azure SQL Database", "Databases", "Relational Databases"),
    ("SQL Managed Instance", "Azure SQL Managed Instance", "Databases", "Relational Databases"),
    ("Azure Database for PostgreSQL", "Azure Database for PostgreSQL", "Databases", "Relational Databases"),
    ("Azure Database for MySQL", "Azure Database for MySQL", "Databases", "Relational Databases"),
    ("Azure Cosmos DB", "Azure Cosmos DB", "Databases", "NoSQL Databases"),
    ("Redis Cache", "Azure Cache for Redis", "Databases", "Caching"),
    ("Azure Synapse Analytics", "Azure Synapse Analytics", "Analytics", "Analytics Platforms"),
    ("Azure Databricks", "Azure Databricks", "Analytics", "Analytics Platforms"),
    ("Azure Data Factory v2", "Azure Data Factory", "Analytics", "Data Processing"),
    ("Event Hubs", "Event Hubs", "Analytics", "Streaming Analytics"),
    ("Azure Cognitive Search", "Azure AI Search", "Analytics", "Search"),
    ("Power BI Embedded", "Power BI Embedded", "Analytics", "Business Intelligence"),
    ("Virtual Network", "Virtual Network", "Networking", "Network Infrastructure"),
    ("Bandwidth", "Bandwidth", "Networking", "Network Connectivity"),
    ("Load Balancer", "Load Balancer", "Networking", "Application Networking"),
    ("Application Gateway", "Application Gateway", "Networking", "Application Networking"),
    ("VPN Gateway", "VPN Gateway", "Networking", "Network Connectivity"),
    ("ExpressRoute", "ExpressRoute", "Networking", "Network Connectivity"),
    ("Azure DNS", "Azure DNS", "Networking", "Network Routing"),
    ("Content Delivery Network", "Content Delivery Network", "Networking", "Content Delivery"),
    ("Azure Front Door Service", "Azure Front Door", "Networking", "Content Delivery"),
    ("Azure Firewall", "Azure Firewall", "Networking", "Network Security"),
    ("Service Bus", "Service Bus", "Integration", "Messaging"),
    ("Logic Apps", "Logic Apps", "Integration", "Workflow Orchestration"),
    ("API Management", "API Management", "Integration", "API Management"),
    ("Azure Monitor", "Azure Monitor", "Management and Governance", "Observability"),
    ("Log Analytics", "Log Analytics", "Management and Governance", "Observability"),
    ("Microsoft Defender for Cloud", "Microsoft Defender for Cloud", "Security", "Security Posture Management"),
    ("Key Vault", "Key Vault", "Security", "Secret Management"),
    ("Microsoft Entra ID", "Microsoft Entra ID", "Identity", "Identity and Access Management"),
    ("Cognitive Services", "Azure AI Services", "AI and Machine Learning", "AI Platforms"),
    ("Azure OpenAI Service", "Azure OpenAI Service", "AI and Machine Learning", "Generative AI"),
    ("Azure Machine Learning", "Azure Machine Learning", "AI and Machine Learning", "Machine Learning"),
    ("IoT Hub", "IoT Hub", "Internet of Things", "IoT Platforms"),
    ("Azure DevOps", "Azure DevOps", "Developer Tools", "Developer Platforms"),
]

_GCP = [
    ("Compute Engine", "Compute Engine", "Compute", "Virtual Machines"),
    ("Kubernetes Engine", "Google Kubernetes Engine", "Compute", "Containers"),
    ("Cloud Run", "Cloud Run", "Compute", "Serverless Compute"),
    ("Cloud Run Functions", "Cloud Run functions", "Compute", "Serverless Compute"),
    ("Cloud Functions", "Cloud Functions", "Compute", "Serverless Compute"),
    ("App Engine", "App Engine", "Web", "Application Platforms"),
    ("Artifact Registry", "Artifact Registry", "Developer Tools", "Source Code Management"),
    ("Cloud Build", "Cloud Build", "Developer Tools", "Continuous Integration and Deployment"),
    ("Cloud Storage", "Cloud Storage", "Storage", "Object Storage"),
    ("Filestore", "Filestore", "Storage", "File Storage"),
    ("Backup and DR Service", "Backup and DR Service", "Storage", "Backup Storage"),
    ("Cloud SQL", "Cloud SQL", "Databases", "Relational Databases"),
    ("Cloud Spanner", "Spanner", "Databases", "Relational Databases"),
    ("AlloyDB", "AlloyDB for PostgreSQL", "Databases", "Relational Databases"),
    ("Cloud Bigtable", "Bigtable", "Databases", "NoSQL Databases"),
    ("Cloud Firestore", "Firestore", "Databases", "NoSQL Databases"),
    ("Cloud Memorystore for Redis", "Memorystore for Redis", "Databases", "Caching"),
    ("BigQuery", "BigQuery", "Databases", "Data Warehouses"),
    ("Dataflow", "Dataflow", "Analytics", "Data Processing"),
    ("Cloud Dataproc", "Dataproc", "Analytics", "Data Processing"),
    ("Looker", "Looker", "Analytics", "Business Intelligence"),
    ("Cloud Pub/Sub", "Pub/Sub", "Integration", "Messaging"),
    ("Workflows", "Workflows", "Integration", "Workflow Orchestration"),
    ("Apigee", "Apigee API Management", "Integration", "API Management"),
    ("Networking", "Networking", "Networking", "Network Infrastructure"),
    ("Cloud Load Balancing", "Cloud Load Balancing", "Networking", "Application Networking"),
    ("Cloud CDN", "Cloud CDN", "Networking", "Content Delivery"),
    ("Cloud DNS", "Cloud DNS", "Networking", "Network Routing"),
    ("Cloud Armor", "Cloud Armor", "Networking", "Network Security"),
    ("Cloud Logging", "Cloud Logging", "Management and Governance", "Observability"),
    ("Cloud Monitoring", "Cloud Monitoring", "Management and Governance", "Observability"),
    ("Support", "Google Cloud Support", "Management and Governance", "Support"),
    ("Secret Manager", "Secret Manager", "Security", "Secret Management"),
    ("Cloud Key Management Service (KMS)", "Cloud Key Management Service", "Security", "Secret Management"),
    ("Security Command Center", "Security Command Center", "Security", "Security Posture Management"),
    ("Identity Platform", "Identity Platform", "Identity", "Identity and Access Management"),
    ("Vertex AI", "Vertex AI", "AI and Machine Learning", "AI Platforms"),
    ("Gemini API", "Gemini API", "AI and Machine Learning", "Generative AI"),
    ("Cloud Natural Language API", "Cloud Natural Language API", "AI and Machine Learning", "Natural Language Processing"),
]

SERVICES: tuple[Service, ...] = tuple(
    [Service("aws", *r) for r in _AWS] + [Service("azure", *r) for r in _AZURE] + [Service("gcp", *r) for r in _GCP]
)

# Azure ServiceFamily is a coarser fallback when MeterCategory is not in the table.
AZURE_SERVICE_FAMILY: dict[str, tuple[str, str]] = {
    "Compute": ("Compute", "Other (Compute)"),
    "Containers": ("Compute", "Containers"),
    "Storage": ("Storage", "Other (Storage)"),
    "Networking": ("Networking", "Other (Networking)"),
    "Databases": ("Databases", "Other (Databases)"),
    "Analytics": ("Analytics", "Other (Analytics)"),
    "AI + Machine Learning": ("AI and Machine Learning", "Other (AI and Machine Learning)"),
    "Security": ("Security", "Other (Security)"),
    "Management and Governance": ("Management and Governance", "Other (Management and Governance)"),
    "Developer Tools": ("Developer Tools", "Other (Developer Tools)"),
    "Internet of Things": ("Internet of Things", "Other (Internet of Things)"),
    "Integration": ("Integration", "Other (Integration)"),
    "Web": ("Web", "Other (Web)"),
    "Identity": ("Identity", "Other (Identity)"),
    "Migration": ("Migration", "Other (Migration)"),
    "Mixed Reality": ("Media", "Mixed Reality"),
    "Media": ("Media", "Other (Media)"),
}

PROVIDER_ALIASES = {
    "aws": "aws",
    "amazon": "aws",
    "amazon web services": "aws",
    "aws_cur2": "aws",
    "azure": "azure",
    "microsoft": "azure",
    "azure_amortized": "azure",
    "gcp": "gcp",
    "google": "gcp",
    "google cloud": "gcp",
}

_INDEX: dict[tuple[str, str], Service] = {}
for _s in SERVICES:
    _INDEX[(_s.provider, _s.code.lower())] = _s
    _INDEX.setdefault((_s.provider, _s.name.lower()), _s)


def normalize_provider(provider: str | None) -> str | None:
    """Map provider spellings ('AWS', 'Microsoft', 'Google Cloud') to aws/azure/gcp."""
    if provider is None:
        return None
    return PROVIDER_ALIASES.get(provider.strip().lower())


def find(provider: str | None, service: str | None) -> Service | None:
    """Look a service up by provider and code or display name, case-insensitively."""
    p = normalize_provider(provider)
    if p is None or service is None:
        return None
    return _INDEX.get((p, service.strip().lower()))


def category(provider: str | None, service: str | None) -> str | None:
    """FOCUS ServiceCategory; 'Other' for an unknown service, NULL for NULL input."""
    if service is None or provider is None:
        return None
    s = find(provider, service)
    if s is not None:
        return s.category
    if normalize_provider(provider) == "azure" and service in AZURE_SERVICE_FAMILY:
        return AZURE_SERVICE_FAMILY[service][0]
    return "Other"


def subcategory(provider: str | None, service: str | None) -> str | None:
    """FOCUS ServiceSubcategory; 'Other (<category>)' for an unknown service."""
    if service is None or provider is None:
        return None
    s = find(provider, service)
    if s is not None:
        return s.subcategory
    if normalize_provider(provider) == "azure" and service in AZURE_SERVICE_FAMILY:
        return AZURE_SERVICE_FAMILY[service][1]
    return "Other (Other)"
