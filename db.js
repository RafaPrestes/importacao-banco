const { Pool } = require('pg');

const firebirdConfig = {
  host: '192.168.10.254',
  // port: 3050,
  database: 'C:\\Users\\SERVIDOR\\repos\\bancos\\DATA_ACESSO.FDB',
  user: 'SYSDBA',
  password: 'masterkey',
  charset: 'UTF8',
};

// // Configuração do PostgreSQL
const pool = new Pool({
  user: 'postgres',
  host: '161.35.99.133',
  database: 'jkjardins',
  password: 'ac3ss0d3pl0y',
  port: 5432,
});

// banco teste
// const pool = new Pool({
//   user: 'postgres',
//   host: '192.168.10.254',
//   database: 'jkjardins',
//   password: 'docker',
//   port: 5432,
// });

module.exports = {
  firebirdConfig,
  pool,
};