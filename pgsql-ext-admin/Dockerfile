FROM eclipse-temurin:25-jre-alpine

LABEL maintainer="pgsql-ext-spring"

WORKDIR /app

COPY target/pgsql-ext-admin-*.jar app.jar
COPY src/main/resources/json ./resources/json
COPY src/main/resources/application*.yml ./resources/

EXPOSE 8080

ENTRYPOINT ["java", \
  "--enable-preview", \
  "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED", \
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED", \
  "--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED", \
  "--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED", \
  "--add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED", \
  "--add-opens=java.base/java.lang=ALL-UNNAMED", \
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", \
  "--add-opens=java.base/java.io=ALL-UNNAMED", \
  "--add-opens=java.base/java.util=ALL-UNNAMED", \
  "-jar", "app.jar"]
