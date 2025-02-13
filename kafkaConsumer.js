const { Kafka } = require("kafkajs");
const mysql = require('mysql2');

// Load environment variables
require('dotenv').config();

// MySQL connection
const connection = mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME
});

// Connect to MySQL
connection.connect((err) => {
    if (err) {
        console.error('Error connecting to MySQL:', err);
        return;
    }
    console.log('✅ Connected to MySQL');
});

// Kafka configuration
const kafka = new Kafka({
    clientId: "employee-service",
    brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "employee-group" });

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: "employee_topic", fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                const employeeData = JSON.parse(message.value.toString());
                console.log("employeeData");

                // Insert into employees table
                const employeeQuery = `INSERT INTO employees (name, department, salary) VALUES (?, ?, ?)`;
                connection.query(employeeQuery, 
                    [employeeData.name, employeeData.department, employeeData.salary], 
                    (err, results) => {
                        if (err) {
                            console.error("Error inserting employee:", err);
                            return;
                        }
                        console.log("Employee added:", results.insertId);

                        // Insert into attendance table
                        const attendanceQuery = `INSERT INTO attendance (employee_id, status) VALUES (?, ?)`;
                        connection.query(attendanceQuery, 
                            [results.insertId, employeeData.attendance_status || 'Present'], 
                            (err) => {
                                if (err) {
                                    console.error("Error inserting attendance:", err);
                                    return;
                                }
                                console.log("Attendance recorded for employee ID:", results.insertId);
                            }
                        );

                        // Insert into department table (if department does not exist)
                        const departmentQuery = `
                                                 INSERT INTO departments (name)
                                                 SELECT ? 
                                                 WHERE NOT EXISTS (
                                                 SELECT 1 FROM departments WHERE name = ?
                                                )
                                                `;
                        connection.query(departmentQuery, 
                            [employeeData.department.name, employeeData.department.name], 
                            (err) => {
                                if (err) {
                                    console.error("Error inserting department:", err);
                                    return;
                                }
                                console.log("Department checked/added:", employeeData.department);
                            }
                        );
                    }
                );
            } catch (error) {
                console.error("Error processing Kafka message:", error);
            }
        },
    });
};

run().catch(console.error);

