require('dotenv').config();
const express = require('express');
const { pool, poolConnect } = require('./db');

const app = express();
app.use(express.json());

// Pinning, lo mas minimo para ver que la conexcion a la db es correct
app.get('/api/ping', async (_, res) => {
  try {
    await poolConnect;                           
    const { recordset } = await pool.request().query('SELECT 1 AS ok');
    res.json({ db: 'online', result: recordset[0] });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'DB connection failed' });
  }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`🟢 Backend listo en http://localhost:${PORT}`)); // Si se imprime esto, se conecta bien
