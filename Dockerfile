FROM container-registry.zalando.net/library/eclipse-temurin-11-jdk:latest

MAINTAINER Team Aruha, team-aruha@zalando.de

EXPOSE 8080

WORKDIR /

COPY app/build/libs/jolokia-jvm-agent.jar ./
COPY app/build/libs/app.jar nakadi.jar
COPY plugins/authz/build/libs/nakadi-plugin-authz.jar plugins/lightstep/build/libs/nakadi-lightstep.jar plugins/

COPY app/api/nakadi-event-bus-api.yaml api/nakadi-event-bus-api.yaml
COPY app/api/nakadi-event-bus-api.yaml /zalando-apis/nakadi-event-bus-api.yaml

# to make the plugins load, add to JAVA_OPTS: -Dloader.path=/plugins

ENTRYPOINT exec java $JAVA_OPTS \
    -javaagent:/jolokia-jvm-agent.jar=host=0.0.0.0 \
    -Djava.security.egd=file:/dev/./urandom \
    -Dspring.jdbc.getParameterType.ignore=true \
    -jar nakadi.jar
