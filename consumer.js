const { Kafka } = require("kafkajs");

const consumerName = process.argv[2] || "consumer-1";

const kafka = new Kafka({
  clientId: consumerName,
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "payment-group",
});

async function main() {
  await consumer.connect();

  await consumer.subscribe({
    topic: "orders",
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      console.log(
        `[${consumerName}] partition=${partition}, offset=${message.offset}, key=${message.key?.toString()}, value=${message.value?.toString()}`,
      );
    },
  });
}

main().catch(console.error);
