const Firebird = require('node-firebird');
const { firebirdConfig } = require('./db.js');

// // Config do Firebird
// const firebirdConfig = {
//   host: '192.168.10.254',
//   // port: 3050,
//   database: 'C:\\Users\\SERVIDOR\\repos\\bancos\\DATA_ACESSO.FDB',
//   user: 'SYSDBA',
//   password: 'masterkey',
//   charset: 'UTF8',
// };

function queryFirebird(sql) {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, (err, db) => {
      if (err) return reject(err);
      db.query(sql, (err, result) => {
        db.detach();
        if (err) return reject(err);
        resolve(result);
      });
    });
  });
}

// Funções para cada tabela
async function getMoradoresFromFirebird() {
  return queryFirebird('SELECT * FROM TAB_MORADOR WHERE ID_TIPO_MORADOR <> 99');
}

async function getVisitantesFromFirebird() {
  return queryFirebird('SELECT * FROM TAB_PRESTADOR');
}

async function getClassificacoesFromFirebird() {
  return queryFirebird('SELECT * FROM TAB_TIPO_MORADOR');
}

async function getEventosFromFirebird() {
  return queryFirebird('SELECT * FROM TAB_EVENTO_ONLINE');
}

async function getUnidades() {
  return queryFirebird('SELECT * FROM TAB_UNIDADE');
}

async function getUnidadesGrupos() {
  return queryFirebird('SELECT * FROM TAB_GRUPO_UNIDADE');
}

async function getUnidadesStatus() {
  return queryFirebird('SELECT * FROM TAB_TIPO_UNIDADE');
}

async function getVeiculosMoradores() {
  return queryFirebird('SELECT * FROM TAB_VEICULO_MORADOR');
}

async function getVeiculosVisitantes() {
  return queryFirebird('SELECT * FROM TAB_VEICULO_VISITANTE');
}

async function getPets() {
  return queryFirebird('SELECT * FROM TAB_ANIMAL_DOMESTICO');
}

async function getOcorrencias() {
  return queryFirebird('SELECT * FROM TAB_OCORRENCIA');
}

async function getComunicados() {
  return queryFirebird('SELECT * FROM COMUNICADO');
}

async function getCorrespondencias() {
  return queryFirebird('SELECT * FROM TAB_ENTREGA_AVISO');
}

async function getDispositivos() {
  return queryFirebird('SELECT * FROM TAB_DISPOSITIVO');
}

async function getCameras() {
  return queryFirebird('SELECT * FROM TAB_CAMERA');
}

async function getAcessosTipos() {
  return queryFirebird('SELECT * FROM TAB_TIPO_ACESSO');
}

async function getLiberacoes() {
  return queryFirebird('SELECT * FROM TAB_ACESSO_PRESTADOR');
}

module.exports = {
  getMoradoresFromFirebird,
  getClassificacoesFromFirebird,
  getEventosFromFirebird,
  getVisitantesFromFirebird,
  getUnidades,
  getUnidadesGrupos,
  getUnidadesStatus,
  getVeiculosMoradores,
  getVeiculosVisitantes,
  getPets,
  getOcorrencias,
  getComunicados,
  getCorrespondencias,
  getDispositivos,
  getCameras,
  getAcessosTipos,
  getLiberacoes
};
