const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'consumer',
  brokers: ['localhost:9092']  // Substitua pelo endereço do seu broker Kafka
});

const consumer = kafka.consumer({ groupId: 'test-group' });

const run = async () => {
  // Conecta o consumidor
  await consumer.connect();
  
  // Se inscreve no tópico 'test-topic'
  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });
  
  // Consome as mensagens do tópico
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      });
    },
  });
};

run().catch(console.error);