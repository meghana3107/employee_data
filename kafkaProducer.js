const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const { Kafka } = require('kafkajs');

const app = express();
const port = process.env.PORT || 5000;

// Enable CORS for frontend (React)
app.use(cors({
  origin: 'http://localhost:3000', // Allow requests from React frontend
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: true
}));

// Parse JSON request bodies
app.use(bodyParser.json());

// Kafka Configuration
const kafka = new Kafka({
  clientId: 'employee-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
});

const producer = kafka.producer();

// Connect Kafka Producer
const connectProducer = async () => {
  try {
    await producer.connect();
    console.log('✅ Kafka Producer connected');
  } catch (error) {
    console.error('Error connecting Kafka Producer:', error);
  }
};

// Handle Preflight Requests
app.options('*', cors());

// Route to handle employee records
app.post('/api/employees', async (req, res) => {
  try {
    const message = JSON.stringify(req.body);
    console.log('Received Employee Data:', message);
    
    await producer.send({
      topic: 'employee_topic',
      messages: [{ value: message }],
    });

    res.status(201).send({ message: 'Record sent to Kafka successfully' });
  } catch (error) {
    console.error('Error producing message:', error);
    res.status(500).send({ error: 'Error producing message' });
  }
});

// Start Express Server
app.listen(port, () => {
  console.log(`✅ Kafka Producer API listening on port ${port}`);
});

// Connect the Kafka Producer
connectProducer();
