// app.js

require('dotenv').config();
const amqp = require('amqplib');
const nodemailer = require('nodemailer');
const { Client } = require('pg');

// --- Database Configuration ---
const dbClient = new Client({
  host: process.env.PGHOST || 'localhost',
  port: process.env.PGPORT ? parseInt(process.env.PGPORT) : 5432,
  user: process.env.PGUSER || 'your_db_user',
  password: process.env.PGPASSWORD || 'your_db_password',
  database: process.env.PGDATABASE || 'your_db_name'
});

dbClient.connect()
  .then(() => console.log('Connected to PostgreSQL for email logging.'))
  .catch((err) => {
    console.error('Failed to connect to database:', err);
    process.exit(1);
  });

// --- Configure Primary SMTP Transporter ---
const primaryTransportOptions = {
  host: process.env.AWS_SMTP_HOST || 'smtp.aws-region.amazonaws.com',
  port: process.env.AWS_SMTP_PORT ? parseInt(process.env.AWS_SMTP_PORT) : 587,
  secure: process.env.AWS_SMTP_PORT == 465, // Use true if port 465, false otherwise.
  auth: {
    user: process.env.AWS_SMTP_USER,
    pass: process.env.AWS_SMTP_PASSWORD
  },
  // Additional options can be added here (e.g., connectionTimeout)
};

const primaryTransporter = nodemailer.createTransport(primaryTransportOptions);

// --- Log Email Event Function ---
/**
 * Logs an email attempt to the database.
 * @param {Object} logData - Details of the email attempt.
 * @param {string} logData.recipient
 * @param {string} logData.subject
 * @param {string|null} logData.message_id - Message ID from Nodemailer on success
 * @param {string} logData.status - 'sent' or 'failed'
 * @param {string|null} logData.error - Error message, if any
 */
async function logEmailEvent({ recipient, subject, message_id, status, error = null }) {
  const queryText = `
    INSERT INTO email_logs(recipient, subject, message_id, status, error)
    VALUES ($1, $2, $3, $4, $5)
  `;
  try {
    await dbClient.query(queryText, [recipient, subject, message_id, status, error]);
    console.log(`Logged email event: ${recipient} - ${status}`);
  } catch (err) {
    console.error('Error logging email event:', err);
  }
}

// --- Email Sending Function ---
/**
 * Sends an email using the primary SMTP transporter.
 * @param {Object} emailOptions - Contains properties: from, to, subject, text, and html.
 */
async function sendEmail(emailOptions) {
  try {
    const info = await primaryTransporter.sendMail(emailOptions);
    console.log(`Email successfully sent to ${emailOptions.to} with messageId ${info.messageId}`);
    await logEmailEvent({
      recipient: emailOptions.to,
      subject: emailOptions.subject,
      message_id: info.messageId,
      status: 'sent'
    });
    return info;
  } catch (err) {
    console.error(`Failed to send email to ${emailOptions.to}:`, err);
    await logEmailEvent({
      recipient: emailOptions.to,
      subject: emailOptions.subject,
      message_id: null,
      status: 'failed',
      error: err.message
    });
    throw err;
  }
}

// --- Worker Process: Connect to RabbitMQ and Process Email Tasks ---
async function startWorker() {
  try {
    const rabbitMqUrl = process.env.RABBITMQ_URL || 'amqp://rabbitmq:5672';
    const connection = await amqp.connect(rabbitMqUrl);
    const channel = await connection.createChannel();

    const queue = process.env.EMAIL_QUEUE || 'emailQueue';
    await channel.assertQueue(queue, { durable: true });
    console.log(`[*] Waiting for messages in queue: ${queue}`);

    channel.consume(queue, async (msg) => {
      if (msg !== null) {
        try {
          const emailData = JSON.parse(msg.content.toString());
          console.log(`Received email request: ${msg.content.toString()}`);
  
          const emailOptions = {
            from: process.env.EMAIL_FROM || 'no-reply@yourdomain.com',
            to: emailData.to,
            subject: emailData.subject,
            text: emailData.text,
            html: emailData.html
          };
  
          await sendEmail(emailOptions);
          channel.ack(msg);
        } catch (error) {
          console.error('Error processing email message:', error);
          // Optionally, set a retry count or route failed messages to a dead-letter queue.
          channel.nack(msg, false, false);
        }
      }
    });
  } catch (error) {
    console.error('Fatal error in email worker:', error);
    process.exit(1);
  }
}

startWorker();
