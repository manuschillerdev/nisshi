# Builds the OpenMessaging Benchmark from a checkout passed as the build context.
# Upstream's docker/Dockerfile.build runs plain `mvn install`, which fails on spotless/license
# checks for some upstream commits, so we skip those: we only need the binaries.
FROM maven:3.9.9-eclipse-temurin-17 AS build
COPY . /benchmark
WORKDIR /benchmark
# Upstream's Benchmark.main returns without System.exit, and KafkaTopicCreator's non-daemon
# ScheduledExecutorService is never shut down, so the JVM would idle forever after the run
RUN sed -i 's/^        worker.close();$/        worker.close();\n        System.exit(0);/' benchmark-framework/src/main/java/io/openmessaging/benchmark/Benchmark.java \
    && grep -q 'System.exit(0);' benchmark-framework/src/main/java/io/openmessaging/benchmark/Benchmark.java
RUN mvn -q install -DskipTests -Dspotless.check.skip=true -Dlicense.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true

FROM eclipse-temurin:17
COPY --from=build /benchmark/package/target/openmessaging-benchmark-*-SNAPSHOT-bin.tar.gz /
RUN mkdir /benchmark && tar -xzf /openmessaging-benchmark-*-SNAPSHOT-bin.tar.gz -C /benchmark --strip=1 \
    && rm /openmessaging-benchmark-*-SNAPSHOT-bin.tar.gz
WORKDIR /benchmark
CMD ["/bin/bash"]
