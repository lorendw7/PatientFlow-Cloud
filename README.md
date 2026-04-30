# PatientFlow Cloud

## Enterprise-Ready Healthcare Management Microservices Platform

**PatientFlow Cloud** is a production-grade healthcare backend platform built with a Java Spring Boot microservices architecture. It is designed to demonstrate how a modern patient management system can be engineered, tested, containerized, and deployed from local development environments to AWS cloud infrastructure.

The platform focuses on patient-centered healthcare workflows and integrates four core microservices:

- **Patient Service** — manages patient profiles and core medical administration data
- **Billing Service** — handles billing operations and financial workflows
- **Analytics Service** — processes operational and business insights
- **Auth Service** — manages authentication, authorization, and identity security

To support real-world distributed system requirements, the project adopts:

- **gRPC** for synchronous inter-service communication
- **Kafka** for asynchronous event-driven messaging
- **API Gateway** for unified traffic entry and request routing
- **JWT** for secure identity authentication and access control

The system is fully aligned with cloud-native engineering practices through:

- **Docker** for containerization
- **AWS ECS** for scalable service orchestration
- **AWS CloudFormation** for infrastructure provisioning and deployment automation

In addition, the project includes:

- **Unit tests** for service-level reliability
- **Integration tests** for end-to-end validation
- **OpenAPI documentation** for clear API design and developer onboarding

Overall, **PatientFlow Cloud** serves as an implementable backend solution for healthcare digitalization, with an emphasis on **availability, scalability, maintainability, and security**.