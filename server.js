const Firebird = require('node-firebird');
const { Pool } = require('pg');
const tableMappings = require('./columnMapping.json');
const { buildColumnsAndValues } = require('./buildMapeamentos');

// Configuração do Firebird
const firebirdConfig = {
  host: '192.168.0.254',
  // port: 3050,
  database: 'C:\\Users\\SERVIDOR\\repos\\acesso-client\\Banco\\DATA_ACESSO.FDB',
  user: 'SYSDBA',
  password: 'masterkey',
  charset: 'UTF8',
};

// Configuração do PostgreSQL
const pool = new Pool({
  user: 'postgres',
  host: '192.168.0.254',
  database: 'import',
  password: 'docker',
  port: 5432,
});

const especieMap = {
  0: 'caninos',
  1: 'felinos',
  2: 'aves',
  3: 'aquaticos',
  4: 'repteis',
  5: 'outros',
};

const pesoMap = {
  0: 'ate6',
  1: '6a15',
  2: '15a25',
  3: '25a45',
  4: '45a60',
  5: 'outro',
}

// Mapeamento de tabelas e colunas
const tableMapping = [
  {
    firebird: 'TAB_TIPO_MORADOR',
    postgres: 'pessoas_classificacoes',
    columnMapping: tableMappings['TAB_TIPO_MORADOR'],
  },
  {
    firebird: 'TAB_MORADOR',
    postgres: 'pessoas',
    columnMapping: tableMappings['TAB_MORADOR'],
    whereCondition: "id_tipo_morador <> 99"
  },
  {
    firebird: 'TAB_GRUPO_UNIDADE',
    postgres: 'unidades_grupos',
    columnMapping: tableMappings['TAB_GRUPO_UNIDADE'],
  },
  {
    firebird: 'TAB_UNIDADE',
    postgres: 'unidades',
    columnMapping: tableMappings['TAB_UNIDADE'],
  },
  {
    firebird: 'TAB_PRESTADOR',
    postgres: 'pessoas',
    columnMapping: tableMappings['TAB_PRESTADOR'],
  }
];

// Função genérica para inserir dados em uma tabela PostgreSQL
async function insertIntoPostgres(table, columns, values, conflictColumn = null) {
  const client = await pool.connect();
  try {

    const placeholders = columns.map((_, index) => `$${index + 1}`).join(',');

    const query = `
      INSERT INTO ${table} (${columns.join(',')}, created_at, updated_at)
      VALUES (${placeholders}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      RETURNING id;
    `;
    const result = await client.query(query, values);
    return result.rows.length > 0 ? result.rows[0].id : null; // Retorna o ID ou null
  } catch (error) {
    console.error(`Erro ao inserir na tabela ${table}:`, error);
    throw error;
  } finally {
    client.release();
  }
}

async function migrateClassificacoes() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM TAB_TIPO_MORADOR',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar TAB_TIPO_MORADOR: ${err}`);
          }

          const client = await pool.connect();
          try {
            for (const row of result) {

              // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
              const { columns, values } = buildColumnsAndValues(row, 'pessoa_classificacao');

              // Inserir morador na tabela `pessoas_classificacoes`
              await insertIntoPostgres('pessoas_classificacoes', columns, values, 'id_outside');
            }

            console.log('pessoas_classificacoes migrados com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar pessoas_classificacoes: ${error}`);
          } finally {
            client.release();
            firebirdClient.detach();
          }
        }
      );
    });
  });
}

async function migrateMoradores() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM TAB_MORADOR WHERE ID_TIPO_MORADOR <> 99',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar TAB_MORADOR: ${err}`);
          }

          const client = await pool.connect();
          try {
            for (const row of result) {
              const role = 'residente';

              // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
              const { columns, values } = buildColumnsAndValues(row, 'morador');

              columns.push('role');
              values.push(role);

              // Busca o `id da classificacao` no PostgreSQL
              const classificacaoQuery = `SELECT id FROM pessoas_classificacoes WHERE id_outside = $1`;
              const classificacaoResult = await client.query(classificacaoQuery, [row.ID_TIPO_MORADOR]);

              if (classificacaoResult.rows.length === 0) {
                continue;
              }
              const classificacaoId = classificacaoResult.rows[0].id;

              columns.push('classificacao_id');
              values.push(classificacaoId);

              // Inserir morador na tabela `pessoas`
              await insertIntoPostgres('pessoas', columns, values, 'id_outside');
            }

            console.log('Moradores migrados com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar moradores: ${error}`);
          } finally {
            client.release();
            firebirdClient.detach();
          }
        }
      );
    });
  });
}

async function migrateUnidadesGrupos() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM TAB_GRUPO_UNIDADE',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar TAB_GRUPO_UNIDADE: ${err}`);
          }

          const client = await pool.connect();
          try {
            for (const row of result) {

              // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
              const { columns, values } = buildColumnsAndValues(row, 'unidades_grupos');

              // Inserir unidade na tabela `unidades`
              await insertIntoPostgres('unidades_grupos', columns, values, 'id_outside');
            }

            console.log('unidades_grupos migrados com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar unidades_grupos: ${error}`);
          } finally {
            client.release();
            firebirdClient.detach();
          }
        }
      );
    });
  });
}

async function migrateUnidadeStatus() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM TAB_TIPO_UNIDADE',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar TAB_TIPO_UNIDADE: ${err}`);
          }

          const client = await pool.connect();
          try {
            for (const row of result) {

              // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
              const { columns, values } = buildColumnsAndValues(row, 'unidades_status');

              await insertIntoPostgres('unidades_status', columns, values, 'id_outside');
            }

            console.log('unidades_status migrados com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar unidades_status: ${error}`);
          } finally {
            client.release();
            firebirdClient.detach();
          }
        }
      );
    });
  });
}

async function migrateUnidades() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM TAB_UNIDADE',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar TAB_UNIDADE: ${err}`);
          }

          const client = await pool.connect();
          try {
            for (const row of result) {

              // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
              const { columns, values } = buildColumnsAndValues(row, 'unidades');

              // Consulta para o grupo da unidade
              const unidadeGrupoQuery = `SELECT id FROM unidades_grupos WHERE id_outside = $1`;
              const unidadeGrupoResult = await client.query(unidadeGrupoQuery, [row.ID_GRUPO_UNIDADE]);

              if (unidadeGrupoResult.rows.length > 0) {
                const unidadeGrupoId = unidadeGrupoResult.rows[0].id;
                columns.push('grupo_unidade_id');
                values.push(unidadeGrupoId);
              } else {
                // Se não encontrar, insere NULL
                columns.push('grupo_unidade_id');
                values.push(null);
              }

              // Consulta para o status da unidade
              const unidadeStatusQuery = `SELECT id FROM unidades_grupos WHERE id_outside = $1`;
              const unidadeStatusResult = await client.query(unidadeStatusQuery, [row.ID_TIPO_UNIDADE]);

              if (unidadeStatusResult.rows.length > 0) {
                const unidadeStatusId = unidadeStatusResult.rows[0].id;
                columns.push('status_id');
                values.push(unidadeStatusId);
              } else {
                // Se não encontrar, insere NULL
                columns.push('status_id');
                values.push(null);
              }
              // Inserir morador na tabela `pessoas_classificacoes`
              await insertIntoPostgres('unidades', columns, values, 'id_outside');
            }

            console.log('unidades migrados com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar unidades: ${error}`);
          } finally {
            client.release();
            firebirdClient.detach();
          }
        }
      );
    });
  });
}

async function migrateVisitantes() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM TAB_PRESTADOR',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar TAB_PRESTADOR: ${err}`);
          }

          const client = await pool.connect();
          try {
            for (const row of result) {
              const role = 'visitante';

              // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
              const { columns, values } = buildColumnsAndValues(row, 'visitante');

              columns.push('role');
              values.push(role);

              // Inserir visitante na tabela `pessoas`
              await insertIntoPostgres('pessoas', columns, values, 'id_outside');
            }

            console.log('Visitantes migrados com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar visitantes: ${error}`);
          } finally {
            client.release();
            firebirdClient.detach();
          }
        }
      );
    });
  });
}

async function migrateVeiculosMoradores() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM TAB_VEICULO_MORADOR',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar TAB_VEICULO_MORADOR: ${err}`);
          }

          const client = await pool.connect();
          try {
            for (const row of result) {

              // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
              const { columns, values } = buildColumnsAndValues(row, 'veiculo');

              // Busca o `id da unidade` no PostgreSQL
              const unidadeQuery = `SELECT id FROM unidades WHERE id_outside = $1`;
              const unidadeResult = await client.query(unidadeQuery, [row.ID_UNIDADE]);

              if (unidadeResult.rows.length === 0) {
                continue;
              }
              const unidadeId = unidadeResult.rows[0].id;

              columns.push('unidade_id');
              values.push(unidadeId);

              // verifica o tipo de veículo, se é carro ou moto
              const tipoVeiculo = row.ID_TIPO_VEICULO === 'C' ? 'carro' : 'moto';
              columns.push('tipo');
              values.push(tipoVeiculo);

              // Inserir morador na tabela `pessoas`
              await insertIntoPostgres('veiculos', columns, values, 'id_outside');
            }

            console.log('veiculos de moradores migrados com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar veiculos de moradores: ${error}`);
          } finally {
            client.release();
            firebirdClient.detach();
          }
        }
      );
    });
  });
}

async function migrateVeiculosVisitantes() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM TAB_VEICULO_VISITANTE',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar TAB_VEICULO_VISITANTE: ${err}`);
          }

          const client = await pool.connect();
          try {
            for (const row of result) {

              // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
              const { columns, values } = buildColumnsAndValues(row, 'veiculo');

              // Busca o `id do visitante` no PostgreSQL
              const visitanteQuery = `SELECT id FROM pessoas WHERE id_outside = $1 and role = 'visitante`;
              const visitanteResult = await client.query(visitanteQuery, [row.ID_VISITANTE]);

              if (visitanteResult.rows.length === 0) {
                continue;
              }
              const visitanteId = visitanteResult.rows[0].id;

              columns.push('pessoa_id');
              values.push(visitanteId);

              // verifica o tipo de veículo, se é carro ou moto
              const tipoVeiculo = row.ID_TIPO_VEICULO === 'C' ? 'carro' : 'moto';
              columns.push('tipo');
              values.push(tipoVeiculo);

              // Inserir morador na tabela `pessoas`
              await insertIntoPostgres('veiculos', columns, values, 'id_outside');
            }

            console.log('veiculos de visitantes migrados com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar veiculos de visitantes: ${error}`);
          } finally {
            client.release();
            firebirdClient.detach();
          }
        }
      );
    });
  });
}

async function migratePets() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM TAB_ANIMAL_DOMESTICO',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar TAB_ANIMAL_DOMESTICO: ${err}`);
          }

          const client = await pool.connect();
          try {
            for (const row of result) {

              // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
              const { columns, values } = buildColumnsAndValues(row, 'pets');

              const unidadeQuery = `SELECT id FROM unidades WHERE id_outside = $1`;
              const unidadeResult = await client.query(unidadeQuery, [row.ID_UNIDADE]);

              if (unidadeResult.rows.length === 0) {
                continue;
              }
              const unidadeId = unidadeResult.rows[0].id;

              columns.push('unidade_id');
              values.push(unidadeId);


              const especie = especieMap[row.ID_TIPO] || 'outros';

              columns.push('especie');
              values.push(especie);

              const peso = pesoMap[row.ID_PESO] || 'outro';

              columns.push('peso');
              values.push(peso);

              // Inserir animais na tabela `pets`
              await insertIntoPostgres('pets', columns, values, 'id_outside');
            }

            console.log('pets migrados com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar pets: ${error}`);
          } finally {
            client.release();
            firebirdClient.detach();
          }
        }
      );
    });
  });
}

// Função para inserir dados em `pessoas_classificacoes` (Tabela de classificações)
// async function insertClassificacao(row) {
//   const client = await pool.connect();
//   try {
//     // Inserir na tabela `pessoas_classificacoes`
//     const insertQuery = `
//       INSERT INTO pessoas_classificacoes (id_outside, nome, created_at, updated_at)
//       VALUES ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id
//     `;

//     const result = await client.query(insertQuery, [row['ID_TIPO_MORADOR'], row['DS_TIPO_MORADOR']]);
//     return result.rows[0].id; // Retorna o id gerado da classificação
//   } catch (error) {
//     console.error('Erro ao inserir na tabela pessoas_classificacoes:', error);
//     throw error;
//   } finally {
//     client.release();
//   }
// }

// async function migrateUnidadePessoa() {
//   return new Promise((resolve, reject) => {
//     Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
//       if (err) {
//         console.error('Erro ao conectar ao Firebird:', err);
//         return reject(err);
//       }

//       firebirdClient.query('SELECT CD_MORADOR, ID_UNIDADE FROM TAB_MORADOR', async (err, result) => {
//         if (err) {
//           console.error('Erro ao consultar TAB_MORADOR no Firebird:', err);
//           firebirdClient.detach();
//           return reject(err);
//         }

//         const client = await pool.connect();
//         try {
//           for (const row of result) {
//             const { CD_MORADOR, ID_UNIDADE } = row;

//             // Busca o `unidade_id` no PostgreSQL
//             const unidadeQuery = `SELECT id FROM unidades WHERE id_outside = $1`;
//             const unidadeResult = await client.query(unidadeQuery, [ID_UNIDADE]);

//             if (unidadeResult.rows.length === 0) {
//               continue;
//             }
//             const unidadeId = unidadeResult.rows[0].id;

//             // Busca o `pessoa_id` no PostgreSQL
//             const pessoaQuery = `SELECT id FROM pessoas WHERE id_outside = $1 and role = 'residente'`;
//             const pessoaResult = await client.query(pessoaQuery, [CD_MORADOR]);

//             if (pessoaResult.rows.length === 0) {
//               continue;
//             }
//             const pessoaId = pessoaResult.rows[0].id;

//             // Insere na tabela `unidades_pessoas`
//             const insertQuery = `
//               INSERT INTO unidades_pessoas (unidade_id, pessoa_id, created_at, updated_at)
//               VALUES ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
//             `;
//             await client.query(insertQuery, [unidadeId, pessoaId]);
//           }

//           console.log('Dados da tabela TAB_MORADOR migrados para unidades_pessoas');
//           resolve();
//         } catch (error) {
//           console.error('Erro ao migrar unidades_pessoas:', error);
//           reject(error);
//         } finally {
//           client.release();
//           firebirdClient.detach();
//         }
//       });
//     });
//   });
// }

// async function migrateAnimais() {
//   return new Promise((resolve, reject) => {
//     Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
//       if (err) {
//         console.error('Erro ao conectar ao Firebird:', err);
//         return reject(err);
//       }

//       firebirdClient.query('SELECT * FROM TAB_ANIMAL_DOMESTICO', async (err, result) => {
//         if (err) {
//           console.error('Erro ao consultar TAB_ANIMAL_DOMESTICO no Firebird:', err);
//           firebirdClient.detach();
//           return reject(err);
//         }

//         const client = await pool.connect();
//         try {
//           for (const row of result) {
//             const { CD_ANIMAL, ID_UNIDADE, NM_ANIMAL, DS_RACA, ID_TIPO } = row;

//             const especie = especieMap[ID_TIPO] || 'outros';

//             // Busca o `unidade_id` no PostgreSQL
//             const unidadeQuery = `SELECT id FROM unidades WHERE id_outside = $1`;
//             const unidadeResult = await client.query(unidadeQuery, [ID_UNIDADE]);

//             if (unidadeResult.rows.length === 0) {
//               continue;
//             }
//             const unidadeId = unidadeResult.rows[0].id;

//             // Insere na tabela `pets`
//             const insertQuery = `
//               INSERT INTO pets (unidade_id, nome, raca, especie, id_outside, created_at, updated_at)
//               VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
//             `;
//             await client.query(insertQuery, [unidadeId, NM_ANIMAL, DS_RACA, especie, CD_ANIMAL]);
//           }

//           console.log('Dados da tabela TAB_ANIMAL_DOMESTICO migrados para pets');
//           resolve();
//         } catch (error) {
//           console.error('Erro ao migrar pets:', error);
//           reject(error);
//         } finally {
//           client.release();
//           firebirdClient.detach();
//         }
//       });
//     });
//   });
// }

// async function migrateVeiculosMoradores() {
//   return new Promise((resolve, reject) => {
//     Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
//       if (err) {
//         console.error('Erro ao conectar ao Firebird:', err);
//         return reject(err);
//       }

//       firebirdClient.query('SELECT * FROM TAB_VEICULO_MORADOR', async (err, result) => {
//         if (err) {
//           console.error('Erro ao consultar TAB_VEICULO_MORADOR no Firebird:', err);
//           firebirdClient.detach();
//           return reject(err);
//         }

//         const client = await pool.connect();
//         try {
//           for (const row of result) {

//             const { ID_UNIDADE } = row

//             // Busca o `unidade_id` no PostgreSQL
//             const unidadeQuery = `SELECT id FROM unidades WHERE id_outside = $1`;
//             const unidadeResult = await client.query(unidadeQuery, [ID_UNIDADE]);

//             if (unidadeResult.rows.length === 0) {
//               continue;
//             }
//             const unidadeId = unidadeResult.rows[0].id;

//             const tipoVeiculo = row.ID_TIPO_VEICULO === 'C' ? 'carro' : 'moto';

//             // Verifica se `NR_ANO` é numérico; se não for, define como NULL
//             const ano = /^[0-9]+$/.test(row.NR_ANO) ? parseInt(row.NR_ANO, 10) : null;

//             // Insere na tabela `veiculos`
//             const insertQuery = `
//               INSERT INTO veiculos (unidade_id, ativo, liberado, placa, modelo, marca, ano, cor, tipo, tag, cartao, controle_remoto, controle_letra_a,
//               controle_letra_b, controle_letra_c, id_outside, created_at, updated_at)
//               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
//             `;
//             await client.query(insertQuery, [unidadeId, row.ID_INATIVO, row.ID_ACESSO_LIBERADO, row.NR_PLACA, row.NM_MODELO, row.NM_MARCA,
//               ano, row.DS_COR, tipoVeiculo, row.NR_TAG, row.NR_CARTAO, row.NR_CONTROLE_REMOTO, row.ID_CONTROLE_A, row.ID_CONTROLE_B,
//               row.ID_CONTROLE_C, row.CD_VEICULO]);
//           }

//           console.log('Dados da tabela TAB_VEICULO_MORADOR migrados para veiculos');
//           resolve();
//         } catch (error) {
//           console.error('Erro ao migrar veiculos:', error);
//           reject(error);
//         } finally {
//           client.release();
//           firebirdClient.detach();
//         }
//       });
//     });
//   });
// }

// async function migrateVeiculosVisitantes() {
//   return new Promise((resolve, reject) => {
//     Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
//       if (err) {
//         console.error('Erro ao conectar ao Firebird:', err);
//         return reject(err);
//       }

//       firebirdClient.query('SELECT * FROM TAB_VEICULO_VISITANTE', async (err, result) => {
//         if (err) {
//           console.error('Erro ao consultar TAB_VEICULO_VISITANTE no Firebird:', err);
//           firebirdClient.detach();
//           return reject(err);
//         }

//         const client = await pool.connect();
//         try {
//           for (const row of result) {
//             const { ID_VISITANTE } = row;

//             // Busca o `id_outside` no PostgreSQL
//             const pessoasQuery = `SELECT id FROM pessoas WHERE id_outside = $1 and role = 'visitante'`;
//             const pessoasResult = await client.query(pessoasQuery, [ID_VISITANTE]);

//             if (pessoasResult.rows.length === 0) {
//               continue;
//             }
//             const pessoaId = pessoasResult.rows[0].id;

//             const tipoVeiculo = row.ID_TIPO_VEICULO === 'C' ? 'carro' : 'moto';

//             // Verifica se `NR_ANO` é numérico; se não for, define como NULL
//             const ano = /^[0-9]+$/.test(row.NR_ANO) ? parseInt(row.NR_ANO, 10) : null;

//             // Insere na tabela `veiculos`
//             const insertQuery = `
//               INSERT INTO veiculos (pessoa_id, ativo, liberado, placa, modelo, marca, ano, cor, tipo, tag, cartao, controle_remoto, controle_letra_a,
//               controle_letra_b, controle_letra_c, id_outside, created_at, updated_at)
//               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
//             `;
//             await client.query(insertQuery, [pessoaId, row.ID_INATIVO, row.ID_ACESSO_LIBERADO, row.NR_PLACA, row.NM_MODELO, row.NM_MARCA,
//               ano, row.DS_COR, tipoVeiculo, row.NR_TAG, row.NR_CARTAO, row.NR_CONTROLE_REMOTO, row.ID_CONTROLE_A, row.ID_CONTROLE_B,
//               row.ID_CONTROLE_C, row.CD_VEICULO]);
//           }

//           console.log('Dados da tabela TAB_VEICULO_VISITANTE migrados para veiculos');
//           resolve();
//         } catch (error) {
//           console.error('Erro ao migrar veiculos:', error);
//           reject(error);
//         } finally {
//           client.release();
//           firebirdClient.detach();
//         }
//       });
//     });
//   });
// }

// Função para migrar dados de uma tabela específica
// async function migrateTable(firebirdTable, postgresTable, columnMapping = null, whereCondition = '') {
//   return new Promise((resolve, reject) => {
//     Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
//       if (err) {
//         console.error(`Erro ao conectar ao Firebird: ${firebirdTable}`, err);
//         return reject(err);
//       }

//       // Se `whereCondition` for fornecido, adiciona à consulta
//       const whereClause = whereCondition ? `WHERE ${whereCondition}` : '';
//       firebirdClient.query(`SELECT * FROM ${firebirdTable} ${whereClause}`, async (err, result) => {
//         if (err) {
//           console.error(`Erro ao consultar tabela ${firebirdTable}:`, err);
//           firebirdClient.detach();
//           return reject(err);
//         }

//         const client = await pool.connect();
//         try {
//           for (const row of result) {
//             let columns = Object.keys(row);
//             let values = Object.values(row);

//             // Aplica o mapeamento de colunas, se necessário
//             if (columnMapping) {
//               // Filtra as colunas que estão no mapeamento
//               columns = columns.filter((col) => columnMapping[col]); // Só mantém as colunas que tem mapeamento
//               // Atualiza os valores de acordo com o novo mapeamento
//               values = columns.map((col) => row[col]); // Seleciona apenas os valores das colunas mapeadas
//               // Renomeia as colunas de acordo com o mapeamento
//               columns = columns.map((col) => columnMapping[col]);
//             }

//             if (postgresTable === 'pessoas') {
//               let role = 'membro';

//               // Define o valor de `role` baseado na tabela de origem
//               if (firebirdTable === 'TAB_MORADOR') {
//                 role = 'residente';
//               } else if (firebirdTable === 'TAB_PRESTADOR') {
//                 role = 'visitante';
//               }

//               // Iinserindo os dados na tabela `pessoas`
//               columns.push('role');
//               values.push(role);
//             }

//             // Inserir a classificação na tabela `pessoas_classificacoes` e pegar o id
//             if (postgresTable === 'pessoas_classificacoes') {
//               const classificacaoId = await insertClassificacao(row);
//               if (postgresTable === 'pessoas') {
//                 // Ao inserir os dados na tabela `pessoas`, adiciona o `classificacao_id` obtido
//                 columns.push('classificacao_id');
//                 values.push(classificacaoId);
//               }
//             }

//             if (postgresTable === 'unidades_grupos') {
//               const grupoUnidadeId = await insertUnidadesGrupos(row);
//               if (postgresTable === 'unidades') {
//                 // Ao inserir os dados na tabela `unidaes`, adiciona o `grupo_unidade_id` obtido
//                 columns.push('grupo_unidade_id');
//                 values.push(grupoUnidadeId);
//               }
//             }

//             // Checa se o registro já existe na tabela PostgreSQL
//             const checkQuery = `SELECT COUNT(*) FROM ${postgresTable} WHERE ${columns[0]} = $1`;
//             const checkValue = values[0]; // Usando o primeiro valor como identificador (modifique conforme necessário)
//             const checkResult = await client.query(checkQuery, [checkValue]);

//             if (parseInt(checkResult.rows[0].count) > 0) {
//               // console.log(`Registro com ${columns[0]} = ${checkValue} já existe na tabela ${postgresTable}. Pulando a inserção.`);
//               continue;
//             }

//             const processedValues = values.map((value, index) => {
//               const column = columns[index];
//               if (column === 'documento') {
//                 return value || 'N/A'; // Se vazio, retorna "N/A"
//               }
//               if (['cnh_validade', 'dt_nascimento'].includes(column)) {
//                 return value || null;
//               }
//               return value;
//             });

//             const placeholders = columns.map((_, index) => `$${index + 1}`).join(',');
//             const query = `
//               INSERT INTO ${postgresTable} (${columns.join(',')}, created_at, updated_at) 
//               VALUES (${placeholders}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
//             `;

//             // const processedValues = values.map(value => value === null ? 'N/A' : value);
//             try {
//               // Realiza a inserção dos dados
//               await client.query(query, processedValues);
//             } catch (insertError) {
//               // Verifica se o erro é uma violação de chave estrangeira (erro com código 23503)
//               if (insertError.code === '23503') {
//                 continue
//                 // console.warn(`Erro de chave estrangeira ao tentar inserir pessoa_id ${row.pessoa_id} na tabela ${postgresTable}. Ignorando essa linha.`);
//               } else {
//                 console.error(`Erro ao inserir na tabela ${postgresTable}:`, insertError);
//                 throw insertError;
//               }
//             }
//           }

//           console.log(`Dados da tabela ${firebirdTable} migrados para ${postgresTable}`);
//           resolve();
//         } catch (error) {
//           console.error(`Erro ao migrar dados da tabela ${firebirdTable} para ${postgresTable}:`, error);
//           reject(error);
//         } finally {
//           client.release();
//           firebirdClient.detach();
//         }
//       });
//     });
//   });
// }

// Função principal para migrar todas as tabelas
async function migrateAllTables() {
  try {
    console.log('Iniciando migração de TAB_TIPO_MORAODR -> pessoas_classificacoes');
    await migrateClassificacoes();

    console.log('Iniciando migração de TAB_MORADORES -> pessoas');
    await migrateMoradores();

    console.log('Iniciando migração de TAB_GRUPO_UNIDADE -> unidades_grupos');
    await migrateUnidadesGrupos();

    console.log('Iniciando migração de TAB_TIPO_UNIDADE -> unidades_status');
    await migrateUnidadeStatus();

    console.log('Iniciando migração de TAB_UNIDADE -> unidades');
    await migrateUnidades();

    console.log('Iniciando migração de TAB_PRESTADOR -> pessoas...');
    await migrateVisitantes();

    console.log('Iniciando migração de TAB_VEICULO_MORADOR -> veiculos...');
    await migrateVeiculosMoradores();

    console.log('Iniciando migração de TAB_VEICULO_VISITANTE -> veiculos...');
    await migrateVeiculosVisitantes();

    console.log('Iniciando migração de TAB_ANIMAL_DOMESTICO -> pets...');
    await migratePets();

    console.log('Migração concluída com sucesso!');
    // for (const { firebird, postgres, columnMapping, whereCondition } of tableMapping) {
    //   console.log(`Iniciando migração: ${firebird} -> ${postgres}`);
    //   await migrateTable(firebird, postgres, columnMapping, whereCondition);
    // }

    // // Migração específica para unidades_pessoas
    // console.log('Iniciando migração: TAB_MORADOR -> unidades_pessoas');
    // await migrateUnidadePessoa();

    // // Migração específica para pets
    // console.log('Iniciando migração: TAB_ANIMAL_DOMESTICO -> pets');
    // await migrateAnimais();

    // // Migração específica para veiculos (moradores)
    // console.log('Iniciando migração: TAB_VEICULO_MORADOR -> veiculos');
    // await migrateVeiculosMoradores();

    // // Migração específica para veiculos (visitantes)
    // console.log('Iniciando migração: TAB_VEICULO_VISITANTE -> veiculos');
    // await migrateVeiculosVisitantes();

    // console.log('Migração concluída com sucesso!');
  } catch (error) {
    console.error('Erro durante a migração:', error);
  } finally {
    await pool.end(); // Encerra a conexão com o PostgreSQL
  }
}

// Executa a migração
migrateAllTables();
