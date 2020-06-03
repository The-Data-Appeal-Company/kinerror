FROM gradle:6.1-jdk as builder

ARG JAR_FILE

COPY --chown=gradle:gradle . /home/gradle/src

WORKDIR /home/gradle/src
RUN gradle build -x test

RUN mkdir /app
RUN mv /home/gradle/src/$JAR_FILE /app/app.jar

FROM openjdk:8

COPY --from=builder /app/app.jar /app/app.jar
WORKDIR /app

CMD java $JAVA_OPTS -jar /app/app.jar
