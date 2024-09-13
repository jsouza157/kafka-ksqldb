const { Kafka, Partitioners } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'producer',
  brokers: ['localhost:9092']  // Substitua pelo endereço do seu broker Kafka
});

const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner,
});

const run = async () => {
  // Conecta o produtor
  await producer.connect();
  
  // Envia uma mensagem para o tópico 'test-topic'
  await producer.send({
    topic: 'test-topic',
    messages: [
      { value: JSON.stringify({customer: "jeff", product: 123, value: 157}) },
    ],
  });

  console.log('Mensagem enviada com sucesso!');

  // Desconecta o produtor
  await producer.disconnect();
};

run().catch(console.error);