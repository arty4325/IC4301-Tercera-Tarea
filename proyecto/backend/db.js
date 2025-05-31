require('dotenv').config();
const sql = require('mssql');

/*
    Este documento se encarga de conectarse a la base de datos
    Por seguridad y escalabilidad, los credenciales estan en el archivo .env
*/
const pool = new sql.ConnectionPool({
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  server: process.env.DB_SERVER,          
  port: parseInt(process.env.DB_PORT, 10),
  database: process.env.DB_NAME,
  options: {
    encrypt: process.env.DB_ENCRYPT === 'true',
    trustServerCertificate: process.env.DB_TRUST_CERT === 'true'
  }
});

module.exports = { sql, pool, poolConnect: pool.connect() };
