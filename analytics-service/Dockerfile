# 阶段1：构建（builder）
FROM maven:3.9-eclipse-temurin-21 AS builder

# 设置工作目录
WORKDIR /app

# 复制 pom.xml 并下载依赖（利用 Docker 缓存）
COPY pom.xml .
RUN mvn dependency:go-offline -B

# 复制源码
COPY src ./src

# 打包项目（跳过测试，加快构建）
RUN mvn clean package -DskipTests

# 阶段2：运行（runner）
FROM eclipse-temurin:21-jre-alpine AS runner

WORKDIR /app

# 从构建阶段复制打包好的 jar
COPY --from=builder /app/target/analytics-service-0.0.1-SNAPSHOT.jar ./app.jar

# 声明端口（文档作用）
EXPOSE 4002

# 启动命令
ENTRYPOINT ["java", "-jar", "app.jar"]