const Firebird = require('node-firebird');
const { Pool } = require('pg');
const { buildColumnsAndValues } = require('./buildMapeamentos');
const ImageProcessing = require('./imgProcessing');

// Configuração do Firebird
const firebirdConfig = {
  host: '192.168.0.254',
  // port: 3050,
  database: 'C:\\Users\\SERVIDOR\\repos\\bancos\\aracari\\DATA_ACESSO.FDB',
  user: 'SYSDBA',
  password: 'masterkey',
  charset: 'UTF8',
};

// Configuração do PostgreSQL
const pool = new Pool({
  user: 'postgres',
  host: '161.35.99.133',
  database: 'aracari',
  password: 'ac3ss0d3pl0y',
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

const statusLiberacao = {
  0: 'criada',
  1: 'aberta',
  2: 'finalizada',
}

const tipoLiberacao = {
  0: 'periodo',
  1: 'unica',
}

const direcaoMap = {
  1: 'entrada',
  2: 'saida',
  3: 'indisponivel',
}

const dentroMap = {
  0: false,
  1: true,
  2: false,
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
    return null;
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

              try {
                // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
                const { columns, values } = buildColumnsAndValues(row, 'pessoa_classificacao');

                // Inserir morador na tabela `pessoas_classificacoes`
                const insertedId = await insertIntoPostgres('pessoas_classificacoes', columns, values, 'id_outside');

                if (!insertedId) {
                  console.warn(`Registro ignorado: ${JSON.stringify(row)}`);
                }
              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }
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
              try {
                // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
                const { columns, values } = buildColumnsAndValues(row, 'morador');

                columns.push('role');
                values.push(role);

                // RG dos moradores
                const rg = row.NR_RG?.startsWith('NAORG') ? null : row.NR_RG;
                columns.push('rg');
                values.push(rg);

                // CPF dos moradores
                const cpf = row.NR_CPF_CPNJ === null ? 'NÃO DISPONÍVEL' : row.NR_CPF_CPNJ;
                columns.push('documento');
                values.push(cpf);

                // Observação convertida de blob para text
                const observacao = await ImageProcessing.textFromBlob(row.OBS);
                columns.push('obs');
                values.push(observacao);

                // Busca o `id da classificacao` no PostgreSQL
                const classificacaoQuery = `SELECT id FROM pessoas_classificacoes WHERE id_outside = $1`;
                const classificacaoResult = await client.query(classificacaoQuery, [row.ID_TIPO_MORADOR]);

                if (classificacaoResult.rows.length === 0) {
                  continue;
                }
                const classificacaoId = classificacaoResult.rows[0].id;

                columns.push('classificacao_id');
                values.push(classificacaoId);

                // Converte o Base64 em JPEG e envia para o servidor
                const imageBlobMorador = await ImageProcessing.imgToBase64(row.IMG_MORADOR);
                const imageBlobDocumento = await ImageProcessing.imgToBase64(row.IMG_DOCUMENTO)

                if (imageBlobMorador) {
                  const imagePath = await ImageProcessing.base64ToJPEG(imageBlobMorador);

                  const fotoFaceId = await ImageProcessing.sendImageToServer(imagePath);

                  // Agora adicionamos o `foto_face_id` na tabela `pessoas`
                  columns.push('foto_face_id');
                  values.push(fotoFaceId);
                }

                if (imageBlobDocumento) {
                  const imagePathDocumento = await ImageProcessing.base64ToJPEG(imageBlobDocumento);

                  const documentoId = await ImageProcessing.sendImageToServer(imagePathDocumento);

                  columns.push('foto_documento_id');
                  values.push(documentoId);
                }

                // Inserir morador na tabela `pessoas`
                const insertedId = await insertIntoPostgres('pessoas', columns, values, 'id_outside');

                if (!insertedId) {
                  console.warn(`Registro ignorado: ${JSON.stringify(row)}`);
                }
              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }
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

async function migrateProprietarios() {
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
              const role = 'residente';
              try {
                // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
                const { columns, values } = buildColumnsAndValues(row, 'proprietarios');

                columns.push('role');
                values.push(role);

                const documento = 'N/A'
                columns.push('documento');
                values.push(documento);

                const reside = false;
                columns.push('reside');
                values.push(reside);

                // Inserir proprietário na tabela `pessoas`
                const insertedId = await insertIntoPostgres('pessoas', columns, values, 'id_outside');
                console.log(`proprietario de id ${insertedId} inserido com sucesso.`);

                if (!insertedId) {
                  console.warn(`Registro ignorado por falta de dados`);
                }
              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }
            }

            console.log('Proprietário migrados com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar Proprietário: ${error}`);
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
              try {
                // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
                const { columns, values } = buildColumnsAndValues(row, 'unidades_grupos');

                // Inserir unidade na tabela `unidades`
                const insertedId = await insertIntoPostgres('unidades_grupos', columns, values, 'id_outside');

                if (!insertedId) {
                  console.warn(`Registro ignorado: ${JSON.stringify(row)}`);
                }

              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }
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
              try {
                // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
                const { columns, values } = buildColumnsAndValues(row, 'unidades_status');

                const insertedId = await insertIntoPostgres('unidades_status', columns, values, 'id_outside');

                if (!insertedId) {
                  console.warn(`Registro ignorado: ${JSON.stringify(row)}`);
                }

              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }
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
              try {
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
                  values.push(1);
                }

                // Consulta para proprietários
                const proprietarioQuery = `SELECT id FROM pessoas where nome = $1 and role = 'residente'`
                const proprietarioResult = await client.query(proprietarioQuery, [row.NM_PROPRIETARIO]);

                if (proprietarioResult.rows.length > 0) {
                  const proprietarioId = proprietarioResult.rows[0].id;
                  columns.push('prop_pessoa_id');
                  values.push(proprietarioId);
                } else {
                  columns.push('prop_pessoa_id');
                  values.push(null);
                }

                // Inserir unidade na tabela `unidades`
                const insertedId = await insertIntoPostgres('unidades', columns, values, 'id_outside');
                console.log(`inserido unidade de id ${insertedId}`)

                if (!insertedId) {
                  console.warn(`Registro ignorado: ${JSON.stringify(row)}`);
                }

              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }

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

async function migrateUnidadesPessoas() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM TAB_MORADOR where ID_TIPO_MORADOR <> 99',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar TAB_MORADOR: ${err}`);
          }

          const client = await pool.connect(); // Conecta ao pool uma vez
          try {
            for (const row of result) {
              try {
                // Consulta unidade
                const unidadeQuery = `SELECT id FROM unidades WHERE id_outside = $1`;
                const unidadeResult = await client.query(unidadeQuery, [row.ID_UNIDADE]);
                if (unidadeResult.rows.length === 0) {
                  console.warn(`Unidade não encontrada para ID_OUTSIDE: ${row.ID_UNIDADE}`);
                  continue;
                }
                const unidadeId = unidadeResult.rows[0].id;

                // Consulta morador
                const moradorQuery = `SELECT id FROM pessoas WHERE id_outside = $1 AND role = 'residente'`;
                const moradorResult = await client.query(moradorQuery, [row.CD_MORADOR]);
                if (moradorResult.rows.length === 0) {
                  console.warn(`Morador não encontrado para ID_OUTSIDE: ${row.CD_MORADOR}`);
                  continue;
                }
                const moradorId = moradorResult.rows[0].id;

                // Insere na tabela unidades_pessoas
                const query = `
                  INSERT INTO unidades_pessoas (unidade_id, pessoa_id, created_at, updated_at)
                  VALUES ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                  RETURNING unidade_id;
                `;

                await client.query(query, [unidadeId, moradorId]);
              } catch (error) {
                console.error(`Erro ao processar row ID_OUTSIDE=${row.CD_MORADOR}:`, error);
              }
            }
            console.log('Migração de unidades_pessoas concluída com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar unidades_pessoas: ${error}`);
          } finally {
            // Libera o cliente e desconecta do Firebird no final
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
              try {
                const role = 'visitante';

                // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
                const { columns, values } = buildColumnsAndValues(row, 'visitante');

                columns.push('role');
                values.push(role);

                const cpf = row.NR_CPF_CPNJ === null ? 'NÃO DISPONÍVEL' : row.NR_CPF_CPNJ;
                columns.push('documento');
                values.push(cpf);

                // Observação convertida de blob para text
                const observacao = await ImageProcessing.textFromBlob(row.DS_OBS);
                columns.push('obs');
                values.push(observacao);

                // Converte o Base64 em JPEG e envia para o servidor
                const imageBlobVisitante = await ImageProcessing.imgToBase64(row.IMG_FACE);
                const imageBlobDocumento = await ImageProcessing.imgToBase64(row.IMG_DOCUMENTO)

                if (imageBlobVisitante) {
                  const imagePath = await ImageProcessing.base64ToJPEG(imageBlobVisitante);

                  const fotoFaceId = await ImageProcessing.sendImageToServer(imagePath);

                  columns.push('foto_face_id');
                  values.push(fotoFaceId);
                }

                if (imageBlobDocumento) {
                  const imagePathDocumento = await ImageProcessing.base64ToJPEG(imageBlobDocumento);

                  const documentoId = await ImageProcessing.sendImageToServer(imagePathDocumento);

                  columns.push('foto_documento_id');
                  values.push(documentoId);
                }

                // Inserir visitante na tabela `pessoas`
                const insertedId = await insertIntoPostgres('pessoas', columns, values, 'id_outside');

                if (!insertedId) {
                  console.warn(`Registro ignorado: ${JSON.stringify(row)}`);
                }

              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }
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
              try {
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

                // IMAGEM do veículo
                const imageBlobVeiculo = await ImageProcessing.imgToBase64(row.IMG_VEICULO);

                if (imageBlobVeiculo) {
                  const imagePath = await ImageProcessing.base64ToJPEG(imageBlobVeiculo);

                  const veiculoImageId = await ImageProcessing.sendImageToServer(imagePath);

                  columns.push('foto_id');
                  values.push(veiculoImageId);
                }

                // Inserir morador na tabela `pessoas`
                const insertedId = await insertIntoPostgres('veiculos', columns, values, 'id_outside');

                if (!insertedId) {
                  console.warn(`Registro ignorado: ${JSON.stringify(row)}`);
                }

              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }
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
              try {
                // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
                const { columns, values } = buildColumnsAndValues(row, 'veiculo');

                // Busca o `id do visitante` no PostgreSQL
                const visitanteQuery = `SELECT id FROM pessoas WHERE id_outside = $1 and role = 'visitante'`;
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

                // IMAGEM do veículo
                const imageBlobVeiculo = await ImageProcessing.imgToBase64(row.IMG_VEICULO);

                if (imageBlobVeiculo) {
                  const imagePath = await ImageProcessing.base64ToJPEG(imageBlobVeiculo);

                  const veiculoImageId = await ImageProcessing.sendImageToServer(imagePath);

                  columns.push('foto_id');
                  values.push(veiculoImageId);
                }

                // Inserir morador na tabela `pessoas`
                const insertedId = await insertIntoPostgres('veiculos', columns, values, 'id_outside');

                if (!insertedId) {
                  console.warn(`Registro ignorado: ${JSON.stringify(row)}`);
                }

              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }
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
              try {
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

                // IMAGEM do pet
                const imageBlobPet = await ImageProcessing.imgToBase64(row.IMG_ANIMAL);

                if (imageBlobPet) {
                  const imagePath = await ImageProcessing.base64ToJPEG(imageBlobPet);

                  const petImageId = await ImageProcessing.sendImageToServer(imagePath);

                  columns.push('foto_id');
                  values.push(petImageId);
                }

                // Inserir animais na tabela `pets`
                const insertedId = await insertIntoPostgres('pets', columns, values, 'id_outside');

                if (!insertedId) {
                  console.warn(`Registro ignorado: ${JSON.stringify(row)}`);
                }

              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }
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
              try {
                // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
                const { columns, values } = buildColumnsAndValues(row, 'ocorrencia');

                // Inserir ocorrencias na tabela `ocorrencias`
                const insertedId = await insertIntoPostgres('ocorrencias', columns, values, 'id_outside');

                if (!insertedId) {
                  console.warn(`Registro ignorado: ${JSON.stringify(row)}`);
                }

              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }
            }

            console.log('ocorrencias migradas com sucesso.');
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

async function migrateComunicados() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM COMUNICADO',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar COMUNICADO: ${err}`);
          }

          const client = await pool.connect();
          try {
            for (const row of result) {
              try {
                // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
                const { columns, values } = buildColumnsAndValues(row, 'comunicados');

                // IMAGEM do comunicado
                const imageBlobComunicado = await ImageProcessing.imgToBase64(row.IMG_COMUNICADO);

                if (imageBlobComunicado) {
                  const imagePath = await ImageProcessing.base64ToJPEG(imageBlobComunicado);

                  const comunicadoImageId = await ImageProcessing.sendImageToServer(imagePath);

                  columns.push('foto_id');
                  values.push(comunicadoImageId);
                }

                // Inserir comunicados na tabela `comunicados`
                const insertedId = await insertIntoPostgres('comunicados', columns, values, 'id_outside');

                if (!insertedId) {
                  console.warn(`Registro ignorado: ${JSON.stringify(row)}`);
                }

              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }
            }

            console.log('comunicados migrados com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar comunicados: ${error}`);
          } finally {
            client.release();
            firebirdClient.detach();
          }
        }
      );
    });
  });
}

async function migrateCorrespondencias() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM TAB_ENTREGA_AVISO',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar TAB_ENTREGA_AVISO: ${err}`);
          }

          const client = await pool.connect();
          try {
            for (const row of result) {
              try {
                // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
                const { columns, values } = buildColumnsAndValues(row, 'correspondencia');

                // Consulta de unidade
                const unidadeQuery = `SELECT id FROM unidades where id_outside = $1`
                const unidadeResult = await client.query(unidadeQuery, [row.ID_UNIDADE]);

                if (unidadeResult.rows.length > 0) {
                  const unidadeId = unidadeResult.rows[0].id;
                  columns.push('unidade_id');
                  values.push(unidadeId);
                }

                // Consulta de morador entregue
                const moradorQuery = `SELECT id FROM pessoas where id_outside = $1 and role = 'residente'`
                const moradorResult = await client.query(moradorQuery, [row.ID_MORADOR_ENTREGUE]);

                if (moradorResult.rows.length > 0) {
                  const moradorId = moradorResult.rows[0].id;
                  columns.push('pessoa_id_entregue');
                  values.push(moradorId);
                }

                // verificar se a correspondência foi entregue ou não
                const entregueBoolean = row.ID_STATUSO = 1 ? false : true;
                columns.push('entregue');
                values.push(entregueBoolean);

                // Inserir tab_entrega_aviso na tabela `correspondencias`
                const insertedId = await insertIntoPostgres('correspondencias', columns, values, 'id_outside');
                console.log(`inserido correspondência de id: ${insertedId}`);

                if (!insertedId) {
                  console.warn(`Registro ignorado: ${JSON.stringify(row)}`);
                }

              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }
            }

            console.log('correspondências migradas com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar correspondências: ${error}`);
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
              try {
                // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
                const { columns, values } = buildColumnsAndValues(row, 'dispositivo');

                if (row.CD_DISPOSITIVO === 99) {
                  continue;
                }

                // 1 = control id, 2 = argus, 3 = hikvision
                let idFabricante;
                if (row.ID_FABRICANTE === 8) {
                  idFabricante = 3;
                }

                columns.push('fabricante');
                values.push(idFabricante);

                // Letra de controle
                const letra = letraControle[row.ID_LETRA_CONTROLE] || 'outros';
                columns.push('controle_letra');
                values.push(letra);

                // Tipos de liberação (unica, período, ambas)
                const tipoLib = facialTipoLib[row.ID_FACIAL_TIPO_LIB_VIS];
                columns.push('facial_tipos_lib');
                values.push(tipoLib);

                // Inserir dispositivos na tabela `dispositivos`
                const insertedId = await insertIntoPostgres('dispositivos', columns, values, 'id_outside');

                if (!insertedId) {
                  console.warn(`Registro ignorado: ${JSON.stringify(row)}`);
                }

              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }
            }

            console.log('dispositivos migrados com sucesso.');
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

async function migrateCameras() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM TAB_CAMERA',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar TAB_CAMERA: ${err}`);
          }

          const client = await pool.connect();
          try {
            for (const row of result) {
              try {
                // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
                const { columns, values } = buildColumnsAndValues(row, 'cameras');

                // Inserir TAB_CAMERA na tabela `cameras`
                const insertedId = await insertIntoPostgres('cameras', columns, values, 'id_outside');

                if (!insertedId) {
                  console.warn(`Registro ignorado: ${JSON.stringify(row)}`);
                }

              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }
            }

            console.log('cameras migradas com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar cameras: ${error}`);
          } finally {
            client.release();
            firebirdClient.detach();
          }
        }
      );
    });
  });
}

async function migrateLiberacoesAcessosTipo() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM TAB_TIPO_ACESSO',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar TAB_TIPO_ACESSO: ${err}`);
          }

          const client = await pool.connect();
          try {
            for (const row of result) {
              try {
                // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
                const { columns, values } = buildColumnsAndValues(row, 'liberacoesAcessoTipo');

                // Inserir tipo acesso na tabela `liberacoes_acessos_tipos`
                const insertedId = await insertIntoPostgres('liberacoes_acessos_tipos', columns, values, 'id_outside');

                if (!insertedId) {
                  console.warn(`Registro ignorado: ${JSON.stringify(row)}`);
                }

              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }
            }

            console.log('liberacoes_acessos_tipos migrados com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar liberacoes_acessos_tipos: ${error}`);
          } finally {
            client.release();
            firebirdClient.detach();
          }
        }
      );
    });
  });
}


async function migrateLiberacoes() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM TAB_ACESSO_PRESTADOR',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar TAB_ACESSO_PRESTADOR: ${err}`);
          }

          const client = await pool.connect();
          try {
            for (const row of result) {
              try {
                // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
                const { columns, values } = buildColumnsAndValues(row, 'liberacoes');

                const status = statusLiberacao[row.ID_STATUS];
                columns.push('status');
                values.push(status);

                const tipo = tipoLiberacao[row.ID_TIPO_LIBERACAO];
                columns.push('tipo');
                values.push(tipo);

                const dentro = dentroMap[row.ID_STATUS];
                columns.push('dentro');
                values.push(dentro);

                const veiculo = `SELECT id FROM veiculos WHERE id_outside = $1 and pessoa_id is not null`;
                const veiculoResult = await client.query(veiculo, [row.ID_VEICULO_UTILIZADO]);

                if (veiculoResult.rows.length > 0) {
                  const veiculoId = veiculoResult.rows[0].id;
                  columns.push('veiculo_id');
                  values.push(veiculoId);
                } else {
                  columns.push('veiculo_id');
                  values.push(null);
                }

                const visitante = `SELECT id FROM pessoas WHERE id_outside = $1 and role = 'visitante'`;
                const visitanteResult = await client.query(visitante, [row.ID_PRESTADOR]);

                if (visitanteResult.rows.length > 0) {
                  const visitanteId = visitanteResult.rows[0].id;
                  columns.push('pessoa_id');
                  values.push(visitanteId);
                } else {
                  columns.push('pessoa_id');
                  values.push(null);
                }

                const tipoAcesso = `SELECT id FROM liberacoes_acessos_tipos WHERE id_outside = $1`;
                const tipoAcessoResult = await client.query(tipoAcesso, [row.ID_TIPO_ACESSO]);

                const tipoAcessoResultId = tipoAcessoResult.rows[0].id;

                columns.push('acesso_tipo_id');
                values.push(tipoAcessoResultId)

                // Inserir liberacoes na tabela `liberacoes`
                const insertedId = await insertIntoPostgres('liberacoes', columns, values, 'id_outside');

                console.log(`inserido liberação do visitante id ${visitanteResult.rows[0]?.id}`)

                if (!insertedId) {
                  console.warn(`Registro ignorado: ${JSON.stringify(row)}`);
                }

              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }
            }

            console.log('liberacoes migradas com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar liberacoes: ${error}`);
          } finally {
            client.release();
            firebirdClient.detach();
          }
        }
      );
    });
  });
}

async function migrateLiberacoesUnidades() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM TAB_ACESSO_PRESTADOR',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar TAB_ACESSO_PRESTADOR: ${err}`);
          }

          const client = await pool.connect();
          try {
            for (const row of result) {
              try {
                // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
                const { columns, values } = buildColumnsAndValues(row, 'liberacoesUnidades');

                const liberacaoId = `SELECT id FROM liberacoes WHERE id_outside = $1`;
                const liberacaoIdResult = await client.query(liberacaoId, [row.CD_ACESSO]);

                if (liberacaoIdResult.rows.length > 0) {
                  const liberacaoId = liberacaoIdResult.rows[0].id;
                  columns.push('liberacao_id');
                  values.push(liberacaoId);
                }

                const unidade = `SELECT unidade_id FROM unidades_pessoas up inner join pessoas p on up.pessoa_id = p.id WHERE p.id_outside = $1`;
                const unidadeResult = await client.query(unidade, [row.ID_MORADOR]);

                if (unidadeResult.rows.length > 0) {
                  const unidadeId = unidadeResult.rows[0].unidade_id;
                  columns.push('unidade_id');
                  values.push(unidadeId);
                }

                const solicitante = `SELECT id FROM pessoas WHERE id_outside = $1 and role = 'residente'`;
                const solicitanteResult = await client.query(solicitante, [row.ID_MORADOR]);

                if (solicitanteResult.rows.length > 0) {
                  const solicitanteId = solicitanteResult.rows[0].id;
                  columns.push('solicitante_id');
                  values.push(solicitanteId);
                }

                // Inserir liberacoes na tabela `liberacoes_unidades`
                const insertedId = await insertIntoPostgres('liberacoes_unidades', columns, values, 'id_outside');

                console.log(`inserido liberação de id ${liberacaoIdResult.rows[0]?.id} na unidade ${unidadeResult.rows[0]?.unidade_id}`)

                if (!insertedId) {
                  console.warn(`Registro ignorado: ${JSON.stringify(row)}`);
                }

              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }
            }

            console.log('liberacoes_unidades migradas com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar liberacoes_unidades: ${error}`);
          } finally {
            client.release();
            firebirdClient.detach();
          }
        }
      );
    });
  });
}

async function migrateEventos() {
  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
      if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

      firebirdClient.query(
        'SELECT * FROM TAB_EVENTO_ONLINE',
        async (err, result) => {
          if (err) {
            firebirdClient.detach();
            return reject(`Erro ao consultar TAB_EVENTO_ONLINE: ${err}`);
          }

          const client = await pool.connect();
          try {
            for (const row of result) {
              try {
                // função buildColumnsAndValues para gerar as colunas e valores dinamicamente
                const { columns, values } = buildColumnsAndValues(row, 'eventos');

                let pessoaQuery;
                let pessoaResult;

                if (row.ID_TIPO_PESSOA === 0) {
                  // Busca o `id do morador` no PostgreSQL
                  pessoaQuery = `SELECT id FROM pessoas WHERE id_outside = $1 and role = 'residente'`;
                  pessoaResult = await client.query(pessoaQuery, [row.ID_PESSOA]);
                }
                else {
                  // Busca o `id do visitante` no PostgreSQL
                  pessoaQuery = `SELECT id FROM pessoas WHERE id_outside = $1 and role = 'visitante'`;
                  pessoaResult = await client.query(pessoaQuery, [row.ID_PESSOA]);
                }

                if (pessoaResult.rows.length > 0) {
                  const pessoaId = pessoaResult.rows[0].id;
                  columns.push('pessoa_id');
                  values.push(pessoaId);
                }

                // Pegando a liberação id
                const liberacaoId = `SELECT id FROM liberacoes WHERE id_outside = $1`;
                const liberacaoIdResult = await client.query(liberacaoId, [row.ID_ACESSO]);

                if (liberacaoIdResult.rows.length > 0) {
                  const liberacaoId = liberacaoIdResult.rows[0].id;
                  columns.push('liberacao_id');
                  values.push(liberacaoId);
                }

                // Pegando o id do dispositivo
                const dispositivo = `SELECT id FROM dispositivos WHERE id_outside = $1`;
                const dispositivoResult = await client.query(dispositivo, [row.ID_DISPOSITIVO]);

                if (dispositivoResult.rows.length > 0) {
                  const dispositivoId = dispositivoResult.rows[0].id;
                  columns.push('dispositivo_id');
                  values.push(dispositivoId);
                }

                // direção (entrada, saída)
                const direcao = direcaoMap[row.ID_DIRECAO] || 'indisponivel';
                columns.push('direcao');
                values.push(direcao);

                // Inserir liberacoes na tabela `liberacoes_unidades`
                const insertedId = await insertIntoPostgres('eventos', columns, values, 'id_outside');
                console.log(`inserido evento de id ${liberacaoIdResult.rows[0]?.id}`)

                if (!insertedId) {
                  console.warn(`Registro ignorado: ${JSON.stringify(row)}`);
                }

              } catch (error) {
                console.error(`Erro ao processar registro ${JSON.stringify(row)}:`, error);
              }
            }

            console.log('eventos migrados com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao migrar eventos: ${error}`);
          } finally {
            client.release();
            firebirdClient.detach();
          }
        }
      );
    });
  });
}

async function updateObservacoes() {
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

              // Busca o `id do visitante` no PostgreSQL
              const visitanteQuery = `SELECT id FROM pessoas WHERE id_outside = $1 and role = 'visitante'`;
              const visitanteResult = await client.query(visitanteQuery, [row.CD_PRESTADOR]);

              if (visitanteResult.rows.length === 0) {
                continue;
              }
              const visitanteId = visitanteResult.rows[0].id;

              const observacao = await ImageProcessing.textFromBlob(row.DS_OBS);

              // Insere na tabela unidades_pessoas
              const query = `
                  UPDATE pessoas set obs = $1 where id_outside = $2 and role = 'visitante';
                `;

              await client.query(query, [observacao, visitanteId]);
            }

            console.log('observações alteradas com sucesso.');
            resolve();
          } catch (error) {
            reject(`Erro ao alterar observações: ${error}`);
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

    console.log('Iniciando migração de TAB_UNIDADES -> pessoas');
    await migrateProprietarios();

    console.log('Iniciando migração de TAB_GRUPO_UNIDADE -> unidades_grupos');
    await migrateUnidadesGrupos();

    console.log('Iniciando migração de TAB_TIPO_UNIDADE -> unidades_status');
    await migrateUnidadeStatus();

    console.log('Iniciando migração de TAB_UNIDADE -> unidades');
    await migrateUnidades();

    console.log('Iniciando migração de TAB_MORADOR -> unidades_pessoas');
    await migrateUnidadesPessoas();

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

    console.log('Iniciando migração de COMUNICADOS -> comunicados...');
    await migrateComunicados();

    console.log('Iniciando migração de TAB_ENTREGA_AVISO -> correspondencias...');
    await migrateCorrespondencias();

    console.log('Iniciando migração de TAB_DISPOSITIVO -> dispositivos...');
    await migrateDispositivos();

    console.log('Iniciando migração de TAB_CAMERA -> cameras...');
    await migrateCameras();

    console.log('Iniciando migração de TAB_TIPO_ACESSO -> liberacoes_acessos_tipos...');
    await migrateLiberacoesAcessosTipo();

    console.log('Iniciando migração de TAB_ACESSO_PRESTADOR -> liberacoes...');
    await migrateLiberacoes();

    console.log('Iniciando migração de TAB_ACESSO_PRESTADOR -> liberacoes_unidades...');
    await migrateLiberacoesUnidades();

    console.log('Iniciando migração de TAB_EVENTO_ONLINE -> eventos...');
    await migrateEventos();

    console.log('Migração concluída com sucesso!');
  } catch (error) {
    console.error('Erro durante a migração:', error);
  } finally {
    await pool.end(); // Encerra a conexão com o PostgreSQL
  }
}

// Executa a migração
migrateAllTables();
