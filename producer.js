const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "order-service",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();

async function sendMessage() {
  await producer.connect();

  for (let i = 1; i <= 5; i++) {
    const order = {
      orderId: i,
      customerId: `customer-${i}`,
      amount: i * 100,
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

sendMessage();
