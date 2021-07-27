FROM openjdk:latest

RUN mkdir --parents /usr/src/app
WORKDIR /usr/src/app

ENV message_broker_address="activemq:61616"
ENV couchbase_address="couchbase"

ADD . /usr/src/app

RUN wget https://mrharibo.github.io/wait-for-it.sh
RUN chmod +x wait-for-it.sh

CMD ./wait-for-it.sh $message_broker_address -- java -cp ./target/classes:./target/lib/* ServiceImpl