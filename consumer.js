const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "payment-group",
});

async function consumeMessages() {
  await consumer.connect();

  await consumer.subscribe({
    topic: "orders",
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        key: message.key.toString(),
        value: message.value.toString(),
      });
    },
  });
}

consumeMessages();
