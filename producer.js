const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "order-service",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();

async function main() {
  await producer.connect();

  for (let i = 1; i <= 30; i++) {
    const order = {
      orderId: `order-${i}`,
      customerId: `customer-${i % 5}`,
      amount: i * 100,
      createdAt: new Date().toISOString(),
    };

    await producer.send({
      topic: "orders",
      messages: [
        {
          key: order.customerId,
          value: JSON.stringify(order),
        },
      ],
    });

    console.log("Sent:", order);
  }

  await producer.disconnect();
}

main().catch(console.error);
