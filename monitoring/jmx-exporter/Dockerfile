FROM confluentinc/cp-kafka:7.5.0

ARG JMX_EXPORTER_VERSION=1.6.0

USER root

RUN mkdir -p /opt/jmx-exporter \
    && curl -fL \
       "https://github.com/prometheus/jmx_exporter/releases/download/v${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar" \
       -o /opt/jmx-exporter/jmx_prometheus_javaagent.jar \
    && chown -R appuser:appuser /opt/jmx-exporter


    


COPY kafka.yml /opt/jmx-exporter/kafka.yml

USER appuser
