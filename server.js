const Firebird = require('node-firebird');
const { Pool } = require('pg');
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

const letraControle = {
  1: 'a',
  2: 'b',
  3: 'c',
  4: 'd',
  5: 'outros',
}

const facialTipoLib = {
  1: 'unica',
  2: 'periodo',
  3: 'ambas',
}

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

              // inserindo a espécie baseado no id vindo do firebird
              const especie = especieMap[row.ID_TIPO] || 'outros';
              columns.push('especie');
              values.push(especie);

              // inserindo o peso baseado no id vindo do firebird
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


async function migrateOcorrencias() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM TAB_OCORRENCIA',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar TAB_OCORRENCIA: ${err}`);
          }

          const client = await pool.connect();
          try {
            for (const row of result) {

              // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
              const { columns, values } = buildColumnsAndValues(row, 'ocorrencia');

              // Inserir animais na tabela `ocorrencias`
              await insertIntoPostgres('ocorrencias', columns, values, 'id_outside');
            }

            console.log('ocorrencias migrado com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar ocorrencias: ${error}`);
          } finally {
            client.release();
            firebirdClient.detach();
          }
        }
      );
    });
  });
}

async function migrateDispositivos() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM TAB_DISPOSITIVO',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar TAB_DISPOSITIVO: ${err}`);
          }

          const client = await pool.connect();
          try {
            for (const row of result) {

              // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
              const { columns, values } = buildColumnsAndValues(row, 'dispositivo');

              // Letra de controle
              const letra = letraControle[row.ID_LETRA_CONTROLE] || 'outros';
              columns.push('controle_letra');
              values.push(letra);

              // Tipos de liberação (unica, período, ambas)
              const tipoLib = facialTipoLib[row.ID_FACIAL_TIPO_LIB_VIS];
              columns.push('facial_tipos_lib');
              values.push(tipoLib);

              // Inserir animais na tabela `dispositivos`
              await insertIntoPostgres('dispositivos', columns, values, 'id_outside');
            }

            console.log('dispositivos migrado com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar dispositivos: ${error}`);
          } finally {
            client.release();
            firebirdClient.detach();
          }
        }
      );
    });
  });
}

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

    console.log('Iniciando migração de TAB_OCORRENCIA -> ocorrencias...');
    await migrateOcorrencias();

    console.log('Iniciando migração de TAB_DISPOSITIVO -> dispositivos...');
    await migrateDispositivos();

    console.log('Migração concluída com sucesso!');
  } catch (error) {
    console.error('Erro durante a migração:', error);
  } finally {
    await pool.end(); // Encerra a conexão com o PostgreSQL
  }
}

// Executa a migração
migrateAllTables();
