const express = require('express');
const cors = require('cors');
const { Kafka } = require('kafkajs');

const app = express();
const port = process.env.PORT || 5000;

// ✅ Enable CORS
app.use(cors({
  origin: 'http://localhost:3000', // Adjust if frontend runs on a different port
  methods: ['GET', 'POST', 'PUT', 'DELETE'],
  allowedHeaders: ['Content-Type']
}));

app.use(express.json());

// Kafka Producer Setup
const kafka = new Kafka({
  clientId: 'employee-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
});

const producer = kafka.producer();

// Connect the Kafka Producer
const runProducer = async () => {
  await producer.connect();
  //console.log('✅ Kafka Producer connected');
};

// API Endpoint to Receive Data and Send to Kafka
app.post('/api/employees', async (req, res) => {
    console.log("Sending data to Kafka Producer:", req.body);
  try {
    console.log('Received request body:', req.body);

    // Send data to Kafka
    await producer.send({
      topic: 'employee_topic',
      messages: [{ value: JSON.stringify(req.body) }]
    });

    res.status(200).send('Employee record sent to Kafka');
  } catch (error) {
    console.error('Error sending record to Kafka:', error);
    res.status(500).send('Failed to send employee record');
  }
});

// Start Express Server and Kafka Producer
app.listen(port, async () => {
  console.log(`✅ Server listening on port ${port}`);
  await runProducer();
});
