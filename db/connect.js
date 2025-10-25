import mysql from "mysql2/promise";

export const pool = mysql.createPool({
    host: "localhost",
    user: "root",
    password: "123456",
    database: "journeypro",
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});
