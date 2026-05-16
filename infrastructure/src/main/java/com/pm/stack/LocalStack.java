// 包路径：项目代码存放位置
package com.pm.stack;

// 导入Java基础工具类
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// 导入 AWS CDK 核心依赖
import software.amazon.awscdk.App;
import software.amazon.awscdk.AppProps;
import software.amazon.awscdk.BootstraplessSynthesizer;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.Token;

// 导入 AWS 网络服务 VPC
import software.amazon.awscdk.services.ec2.ISubnet;
import software.amazon.awscdk.services.ec2.InstanceClass;
import software.amazon.awscdk.services.ec2.InstanceSize;
import software.amazon.awscdk.services.ec2.InstanceType;
import software.amazon.awscdk.services.ec2.Vpc;

// 导入 AWS ECS 容器服务
import software.amazon.awscdk.services.ecs.AwsLogDriverProps;
import software.amazon.awscdk.services.ecs.CloudMapNamespaceOptions;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.ContainerDefinitionOptions;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.FargateService;
import software.amazon.awscdk.services.ecs.FargateTaskDefinition;
import software.amazon.awscdk.services.ecs.LogDriver;
import software.amazon.awscdk.services.ecs.PortMapping;
import software.amazon.awscdk.services.ecs.Protocol;
import software.amazon.awscdk.services.ecs.patterns.ApplicationLoadBalancedFargateService;

// 导入 AWS 日志服务 CloudWatch
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.logs.RetentionDays;

// 导入 AWS Kafka 服务 MSK
import software.amazon.awscdk.services.msk.CfnCluster;

// 导入 AWS 数据库服务 RDS PostgreSQL
import software.amazon.awscdk.services.rds.Credentials;
import software.amazon.awscdk.services.rds.DatabaseInstance;
import software.amazon.awscdk.services.rds.DatabaseInstanceEngine;
import software.amazon.awscdk.services.rds.PostgresEngineVersion;
import software.amazon.awscdk.services.rds.PostgresInstanceEngineProps;

// 导入 AWS 健康检查服务 Route53
import software.amazon.awscdk.services.route53.CfnHealthCheck;

/**
 * 本地微服务部署栈
 * 作用：使用 CDK 代码定义所有云资源，在 LocalStack 中一键创建完整系统
 * 包含：VPC、PostgreSQL、MSK(Kafka)、ECS 微服务、API网关
 */
public class LocalStack extends Stack {
    // 私有网络：所有服务都在这个局域网内运行
    private final Vpc vpc;

    // ECS 容器集群：所有微服务都部署在这个集群里
    private final Cluster ecsCluster;

    /**
     * 构造方法：CDK 入口，初始化所有资源
     * @param scope   CDK 应用对象
     * @param id      栈名称
     * @param stackProps 栈配置
     */
    public LocalStack(final App scope, final String id, final StackProps stackProps) {
        super(scope, id, stackProps);

        // 1. 创建私有网络 VPC
        this.vpc = createVpc();

        // 2. 创建两个 PostgreSQL 数据库：认证服务库、患者服务库
        DatabaseInstance authServiceDb = createDatabase("AuthServiceDB", "auth-service-db");
        DatabaseInstance patientServiceDb = createDatabase("patientServiceDB", "patient-service-db");

        // 3. 为数据库创建 TCP 健康检查
        CfnHealthCheck authDbHealthCheck = createDbHealthCheck(authServiceDb, "AuthServiceDBHealthCheck");
        CfnHealthCheck patientDbHealthCheck = createDbHealthCheck(patientServiceDb, "patientServiceDBHealthCheck");

        // 4. 创建 MSK Kafka 集群
        CfnCluster mskCluster = createMskCluster();

        // 5. 创建 ECS 容器集群
        this.ecsCluster = createEcsCluster();

        // 6. 部署认证微服务
        FargateService authService = createFargateService(
                "AuthService",
                "auth-service",
                List.of(4005),        // 暴露端口
                authServiceDb,       // 关联数据库
                Map.of("JWT_SECRET", "j/+yZa85x90w05zOnIk8BuYBIu9+wX/vKkugFIJesIo=") // JWT密钥
        );
        // 依赖：必须先启动数据库和健康检查，再启动服务
        authService.getNode().addDependency(authDbHealthCheck);
        authService.getNode().addDependency(authServiceDb);

        // 7. 部署账单微服务
        FargateService billingService = createFargateService(
                "BillingService",
                "billing-service",
                List.of(4001,9001),
                null,    // 无数据库
                null     // 无额外环境变量
        );

        // 8. 部署数据分析微服务
        FargateService analyticsService = createFargateService(
                "AnalyticsService",
                "analytics-service",
                List.of(4002),
                null,
                null
        );
        // 依赖：必须先启动 Kafka，再启动分析服务
        analyticsService.getNode().addDependency(mskCluster);

        // 9. 部署患者微服务
        FargateService patientService = createFargateService(
                "PatientService",
                "patient-service",
                List.of(4000),
                patientServiceDb,
                Map.of(
                        "BILLING_SERVICE_ADDRESS", "host.docker.internal",
                        "BILLING_SERVICE_GRPC_PORT", "9001"
                )
        );
        // 依赖：数据库、健康检查、账单服务、Kafka
        patientService.getNode().addDependency(patientServiceDb);
        patientService.getNode().addDependency(patientDbHealthCheck);
        patientService.getNode().addDependency(billingService);
        patientService.getNode().addDependency(mskCluster);

        // 10. 部署 API 网关（统一入口）
        createApiGatewayService();
    }

    /**
     * 创建 API 网关服务
     * 作用：对外提供统一入口，转发请求到各个微服务
     */
    private void createApiGatewayService() {
        // 定义容器运行资源：256 CPU / 512M 内存
        FargateTaskDefinition taskDefinition = FargateTaskDefinition.Builder.create(this, "APIGatewayTaskDefinition")
                .cpu(256)
                .memoryLimitMiB(512)
                .build();

        // 容器配置
        ContainerDefinitionOptions containerOptions = ContainerDefinitionOptions.builder()
                .image(ContainerImage.fromRegistry("api-gateway")) // 使用镜像名
                .environment(Map.of(
                        "SPRING_PROFILES_ACTIVE", "prod",
                        "AUTH_SERVICE_URL", "http://host.docker.internal:4005" // 认证服务地址
                ))
                // 暴露端口 4004
                .portMappings(List.of(4004).stream()
                        .map(port -> PortMapping.builder()
                                .containerPort(port)
                                .hostPort(port)
                                .protocol(Protocol.TCP)
                                .build())
                        .toList())
                // 日志配置：输出到 CloudWatch，保存1天
                .logging(LogDriver.awsLogs(AwsLogDriverProps.builder()
                        .logGroup(LogGroup.Builder.create(this, "ApiGatewayLogGroup")
                                .logGroupName("/ecs/api-gateway")
                                .removalPolicy(RemovalPolicy.DESTROY)
                                .retention(RetentionDays.ONE_DAY)
                                .build())
                        .streamPrefix("api-gateway")
                        .build()))
                .build();

        // 把容器加入任务定义
        taskDefinition.addContainer("APIGatewayContainer", containerOptions);

        // 创建带负载均衡的 Fargate 服务（对外可访问）
        ApplicationLoadBalancedFargateService apiGateway = ApplicationLoadBalancedFargateService.Builder.create(this, "APIGatewayService")
                .cluster(ecsCluster)
                .serviceName("api-gateway")
                .taskDefinition(taskDefinition)
                .desiredCount(1)
                .healthCheckGracePeriod(Duration.seconds(60))
                .build();
    }

    /**
     * 创建私有网络 VPC
     * 作用：隔离网络环境，所有服务在内部通信
     */
    private Vpc createVpc() {
        return Vpc.Builder
                .create(this, "PatientManagermentVPC")
                .vpcName("PatientManagermentVPC")
                .maxAzs(2) // 使用 2 个可用区
                .build();
    }

    /**
     * 创建 PostgreSQL 数据库实例
     * @param id 资源ID
     * @param dbName 数据库名
     * @return 数据库实例
     */
    private DatabaseInstance createDatabase(String id, String dbName){
        return DatabaseInstance.Builder
                .create(this, id)
                // 使用 PostgreSQL 17.2
                .engine(DatabaseInstanceEngine.postgres(
                        PostgresInstanceEngineProps.builder()
                                .version(PostgresEngineVersion.VER_17_2)
                                .build()))
                .vpc(vpc) // 加入VPC
                // 实例类型：低成本 t2.micro
                .instanceType(InstanceType.of(InstanceClass.BURSTABLE2, InstanceSize.MICRO))
                .allocatedStorage(20) // 20G 存储空间
                // 自动生成管理员账号密码
                .credentials(Credentials.fromGeneratedSecret("admin_user"))
                .databaseName(dbName)
                // 栈删除时，数据库也删除
                .removalPolicy(RemovalPolicy.DESTROY)
                .build();
    }

    /**
     * 为数据库创建 TCP 健康检查
     * 作用：监控数据库端口是否通，判断是否存活
     */
    private CfnHealthCheck createDbHealthCheck(DatabaseInstance db, String id){
        return CfnHealthCheck.Builder.create(this, id)
                .healthCheckConfig(CfnHealthCheck.HealthCheckConfigProperty.builder()
                        .type("TCP")                // TCP 检查
                        .port(Token.asNumber(db.getDbInstanceEndpointPort())) // 数据库端口
                        .ipAddress(db.getDbInstanceEndpointAddress())        // 数据库地址
                        .requestInterval(30)        // 每30秒检查一次
                        .failureThreshold(3)         // 失败3次判定不健康
                        .build())
                .build();
    }

    /**
     * 创建 MSK Kafka 集群
     * 作用：提供消息队列，用于服务间异步通信
     */
    private CfnCluster createMskCluster(){
        return CfnCluster.Builder.create(this, "MskCluster")
                .clusterName("kafa-cluster")
                .kafkaVersion("2.8.0")
                .numberOfBrokerNodes(1) // 单节点（测试环境）
                // Broker 节点配置
                .brokerNodeGroupInfo(CfnCluster.BrokerNodeGroupInfoProperty.builder()
                        .instanceType("kafka.m5.xlarge")
                        // 放在私有子网，安全不暴露公网
                        .clientSubnets(vpc.getPrivateSubnets().stream()
                                .map(ISubnet::getSubnetId)
                                .collect(Collectors.toList()))
                        .brokerAzDistribution("DEFAULT")
                        .build())
                .build();
    }

    /**
     * 创建 ECS 容器集群
     * 作用：统一管理所有微服务容器
     */
    private Cluster createEcsCluster(){
        return Cluster.Builder.create(this, "PatientManagementCluster")
                .vpc(vpc)
                // 服务发现域名：内部服务通过此域名互相访问
                .defaultCloudMapNamespace(CloudMapNamespaceOptions.builder()
                        .name("patient-management.local")
                        .build())
                .build();
    }

    /**
     * 通用方法：创建一个微服务（Fargate）
     * @param id 服务ID
     * @param imageName 镜像名称
     * @param ports 暴露端口
     * @param db 关联数据库（可为null）
     * @param additionalEnvVars 额外环境变量
     * @return 微服务对象
     */
    private FargateService createFargateService(String id,
                                                String imageName,
                                                List<Integer> ports,
                                                DatabaseInstance db,
                                                Map<String, String> additionalEnvVars) {

        // 定义容器资源
        FargateTaskDefinition taskDefinition = FargateTaskDefinition.Builder.create(this, id + "Task")
                .cpu(256)
                .memoryLimitMiB(512)
                .build();

        // 容器基础配置
        ContainerDefinitionOptions.Builder containerOptions = ContainerDefinitionOptions.builder()
                .image(ContainerImage.fromRegistry(imageName))
                // 端口映射
                .portMappings(ports.stream()
                        .map(port -> PortMapping.builder()
                                .containerPort(port)
                                .hostPort(port)
                                .protocol(Protocol.TCP)
                                .build())
                        .toList())
                // 日志配置
                .logging(LogDriver.awsLogs(AwsLogDriverProps.builder()
                        .logGroup(LogGroup.Builder.create(this, id + "LogGroup")
                                .logGroupName("/ecs/" + imageName)
                                .removalPolicy(RemovalPolicy.DESTROY)
                                .retention(RetentionDays.ONE_DAY)
                                .build())
                        .streamPrefix(imageName)
                        .build()));

        // 环境变量：默认连接 LocalStack 的 Kafka
        Map<String, String> envVars = new HashMap<>();
        envVars.put("SPRING_KAFKA_BOOTSTRAP_SERVERS", "localhost.localstack.cloud:4510, localhost.localstack.cloud:4511, localhost.localstack.cloud:4512");

        // 合并传入的额外环境变量
        if(additionalEnvVars != null){
            envVars.putAll(additionalEnvVars);
        }

        // 如果关联了数据库，自动注入数据库连接信息
        if(db != null){
            envVars.put("SPRING_DATASOURCE_URL", "jdbc:postgresql://%s:%s/%s-db".formatted(
                    db.getDbInstanceEndpointAddress(),
                    db.getDbInstanceEndpointPort(),
                    imageName
            ));
            envVars.put("SPRING_DATASOURCE_USERNAME", "admin_user");
            envVars.put("SPRING_DATASOURCE_PASSWORD", db.getSecret().secretValueFromJson("password").toString());
            envVars.put("SPRING_JPA_HIBERNATE_DDL_AUTO", "update");
            envVars.put("SPRING_SQL_INIT_MODE", "always");
            envVars.put("SPRING_DATASOURCE_HIKARI_INITIALIZATION_FAIL_TIMEOUT", "60000");
        }

        // 设置环境变量并创建容器
        containerOptions.environment(envVars);
        taskDefinition.addContainer(imageName + "Container", containerOptions.build());

        // 创建并返回微服务
        return FargateService.Builder.create(this, id)
                .cluster(ecsCluster)
                .taskDefinition(taskDefinition)
                .assignPublicIp(false) // 不分配公网IP，安全
                .serviceName(imageName)
                .build();
    }

    /**
     * 程序主入口：启动 CDK 并生成部署模板
     */
    public static void main(final String[] args) {
        // 创建 CDK 应用
        App app = new App(AppProps.builder().outdir("./cdk.out").build());

        // 栈配置：LocalStack 不需要 CDK 引导
        StackProps props = StackProps.builder()
                .synthesizer(new BootstraplessSynthesizer())
                .build();

        // 创建栈
        new LocalStack(app,  "localstack", props);

        // 生成 CloudFormation 模板文件
        app.synth();
        System.out.println("App synthesizing in progress...");
    }
}