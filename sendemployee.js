const produceMessage = require('./kafkaProducer');

// Employee Data
const employeeData = {
    type: "employee",
    data: {
        id: 1, 
        name: "Alice",
        department: "Finance",
        salary: 90000
    }
};

// Department Data
const departmentData = {
    type: "department",
    data: {
        id: 1,
        name: "Finance"
    }
};

// Attendance Data
const attendanceData = {
    type: "attendance",
    data: {
        employee_id: 1,  // Ensure this employee exists!
        date: "2025-02-10",
        status: "Present"
    }
};

// Send messages
(async () => {
    await produceMessage(employeeData);
    console.log("✅ Employee sent successfully!");

    await produceMessage(departmentData);
    console.log("✅ Department sent successfully!");

    await produceMessage(attendanceData);
    console.log("✅ Attendance sent successfully!");
})();
