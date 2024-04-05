import { Kafka, Producer } from "kafkajs";
import prismaClient from "./prisma";

const Kafka_var = {
  brokers: process.env.KAFKA_BROKER,
  username: process.env.KAFKA_USERNAME,
  password: process.env.KAFKA_PASSWORD,
  ca: process.env.KAFKA_CA,
};

if (!Kafka_var?.brokers) {
  throw new Error("KAFKA_BROKER is required");
}

const kafka = new Kafka({
  brokers: [Kafka_var.brokers],
  ssl: {
    ca: [Kafka_var.ca || ""],
  },
  sasl: {
    username: Kafka_var.username || "",
    password: Kafka_var.password || "",
    mechanism: "plain",
  },
});

let producer: null | Producer = null;

export async function createProducer() {
  if (producer) return producer;

  const _producer = kafka.producer();
  await _producer.connect();
  producer = _producer;
  return producer;
}

export async function produceMessage(message: string) {
  const producer = await createProducer();
  await producer.send({
    messages: [{ key: `message-${Date.now()}`, value: message }],
    topic: "MESSAGES",
  });
  return true;
}

export async function startMessageConsumer() {
  console.log("Consumer is running..");
  const consumer = kafka.consumer({ groupId: "default" });
  await consumer.connect();
  await consumer.subscribe({ topic: "MESSAGES", fromBeginning: true });

  await consumer.run({
    autoCommit: true,
    eachMessage: async ({ message, pause }) => {
      if (!message.value) return;
      console.log(`New Message Recv..`);
      try {
        await prismaClient.message.create({
          data: {
            text: message.value?.toString(),
          },
        });
      } catch (err) {
        console.log("Something is wrong");
        pause();
        setTimeout(() => {
          consumer.resume([{ topic: "MESSAGES" }]);
        }, 60 * 1000);
      }
    },
  });
}
export default kafka;
