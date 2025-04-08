// app.js (Email Producer API)

require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const amqp = require('amqplib');

const app = express();
app.use(bodyParser.json());

/**
 * Enqueues an email task to RabbitMQ.
 * @param {Object} emailData - Contains properties: to, subject, text, html.
 */
async function sendEmailMessage(emailData) {
  const rabbitMqUrl = process.env.RABBITMQ_URL || 'amqp://rabbitmq:5672';
  let connection;
  try {
    connection = await amqp.connect(rabbitMqUrl);
    const channel = await connection.createChannel();
    const queue = process.env.EMAIL_QUEUE || 'emailQueue';
    await channel.assertQueue(queue, { durable: true });
    const message = JSON.stringify(emailData);
    channel.sendToQueue(queue, Buffer.from(message), { persistent: true });
    console.log(`Email task enqueued: ${message}`);
    await channel.close();
  } catch (error) {
    console.error('Error enqueuing email message:', error);
    throw error;
  } finally {
    if (connection) {
      await connection.close();
    }
  }
}

app.post('/send-email', async (req, res) => {
  const emailData = req.body;
  // to, subject, text, html
  try {
    await sendEmailMessage(emailData);
    res.status(202).json({ message: 'Email task has been enqueued successfully.' });
  } catch (error) {
    res.status(500).json({ message: 'Failed to enqueue email message', error: error.toString() });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Email Producer API listening on port ${PORT}`));
