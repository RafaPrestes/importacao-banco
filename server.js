const { pool } = require('./db.js');
const { bulkInsert } = require('./bulkInsert');

const mapping = require('./mapeamentos')

const {
  getMoradoresFromFirebird,
  getEventosFromFirebird,
  getVisitantesFromFirebird,
  getUnidades,
  getUnidadesGrupos,
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
} = require('./gets');

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

{/* ---------- MIGRAÇÃO DE CLASSIFICAÇÕES ---------- */ }
async function migrateClassificacoes() {
  const rows = await getMoradoresFromFirebird();
  const columns = Object.values(mapping.pessoa_classificacao); // array de colunas

  // Monta os valores
  const valuesArray = rows.map(row => {
    const values = Object.keys(mapping.pessoa_classificacao).map(key => row[key] ?? null);
    return values;
  });

  // Inserir batch no PostgreSQL
  const insertedIds = await bulkInsert('pessoas_classificacoes', columns, valuesArray);

  console.log(`Migração concluída: ${insertedIds.length} classificações inseridas.`);
}

{/* ---------- MIGRAÇÃO DE MORADORES ---------- */ }
async function migrateMoradores() {
  const rows = await getMoradoresFromFirebird();
  const columns = Object.values(mapping.morador); // array de colunas

  // Adiciona colunas obrigatórias de pessoas que não vêm do Firebird
  if (!columns.includes('role')) columns.push('role');
  if (!columns.includes('documento')) columns.push('documento');

  // Monta os valores
  const valuesArray = rows.map(row => {
    const values = Object.keys(mapping.morador).map(key => {
      if (key === 'ID_INATIVO') {
        return row[key] === 0; // true se ativo, false se inativo
      }
      return row[key] ?? null;
    });
    values.push('residente'); // role fixo
    values.push(row.NR_CPF_CPNJ || 'NÃO DISPONÍVEL'); // documento
    return values;
  });

  // Chama seu bulkInsert
  const insertedIds = await bulkInsert('pessoas', columns, valuesArray);

  console.log(`Migração concluída: ${insertedIds.length} moradores inseridos.`);
}

{/* ---------- MIGRAÇÃO DE PROPRIETÁRIOS ---------- */ }
async function migrateProprietarios(batchSize = 2000) {
  const rows = await getUnidades();
  const columnsBase = Object.values(mapping.proprietarios); // colunas do mapping

  // Adiciona colunas fixas
  if (!columnsBase.includes('role')) columnsBase.push('role');
  if (!columnsBase.includes('documento')) columnsBase.push('documento');
  if (!columnsBase.includes('reside')) columnsBase.push('reside');

  // Monta os valores
  const valuesArray = rows.map(row => {
    const values = Object.keys(mapping.proprietarios).map(key => row[key] ?? null);
    values.push('residente');
    values.push('N/A');
    values.push(false);
    return values;
  });

  // Inserção em lotes para não travar a memória
  for (let i = 0; i < valuesArray.length; i += batchSize) {
    const batch = valuesArray.slice(i, i + batchSize);
    await bulkInsert('pessoas', columnsBase, batch);
    console.log(`✅ Batch ${i / batchSize + 1} inserido (${batch.length} registros).`);
  }

  console.log(`Migração concluída: ${valuesArray.length} proprietários inseridos.`);
}

{/* ---------- MIGRAÇÃO DE UNIDADES GRUPOS ---------- */ }
async function migrateUnidadesGrupos(batchSize = 2000) {
  const rows = await getUnidadesGrupos();
  const columnsBase = Object.values(mapping.unidades_grupos); // colunas do mapping

  // Monta os valores
  const valuesArray = rows.map(row => {
    const values = Object.keys(mapping.unidades_grupos).map(key => row[key] ?? null);
    return values;
  });

  // Inserção em lotes para não travar a memória
  for (let i = 0; i < valuesArray.length; i += batchSize) {
    const batch = valuesArray.slice(i, i + batchSize);
    await bulkInsert('unidades_grupos', columnsBase, batch);
    console.log(`✅ Batch ${i / batchSize + 1} inserido (${batch.length} registros).`);
  }

  console.log(`Migração concluída: ${valuesArray.length} unidades_grupos inseridos.`);
}

{/* ---------- MIGRAÇÃO DE UNIDADES STATUS ---------- */ }
async function migrateUnidadeStatus(batchSize = 2000) {
  const rows = await getUnidadesGrupos();
  const columnsBase = Object.values(mapping.unidades_status); // colunas do mapping


  // Monta os valores
  const valuesArray = rows.map(row => {
    const values = Object.keys(mapping.unidades_status).map(key => row[key] ?? null);
    return values;
  });

  // Inserção em lotes para não travar a memória
  for (let i = 0; i < valuesArray.length; i += batchSize) {
    const batch = valuesArray.slice(i, i + batchSize);
    await bulkInsert('unidades_status', columnsBase, batch);
    console.log(`✅ Batch ${i / batchSize + 1} inserido (${batch.length} registros).`);
  }

  console.log(`Migração concluída: ${valuesArray.length} unidades_status inseridos.`);
}

{/* ---------- MIGRAÇÃO DE UNIDADES ---------- */ }
async function migrateUnidades(batchSize = 2000) {
  const rows = await getUnidades();
  const columnsBase = Object.values(mapping.unidades); // colunas do mapping

  // Criar mapas para evitar consultas repetidas
  const client = await pool.connect();
  const gruposMap = new Map();
  const statusMap = new Map();
  const pessoasMap = new Map();

  // Precarregar grupos
  const gruposRes = await client.query('SELECT id_outside, id FROM unidades_grupos');
  gruposRes.rows.forEach(r => gruposMap.set(r.id_outside, r.id));

  // Precarregar status
  const statusRes = await client.query('SELECT id_outside, id FROM unidades_status');
  statusRes.rows.forEach(r => statusMap.set(r.id_outside, r.id));

  // Precarregar pessoas (residentes)
  const pessoasRes = await client.query("SELECT id_outside, id, nome FROM pessoas WHERE role = 'residente'");
  pessoasRes.rows.forEach(r => pessoasMap.set(r.nome, r.id));

  // Adicionar colunas extras
  if (!columnsBase.includes('grupo_unidade_id')) columnsBase.push('grupo_unidade_id');
  if (!columnsBase.includes('status_id')) columnsBase.push('status_id');
  if (!columnsBase.includes('prop_pessoa_id')) columnsBase.push('prop_pessoa_id');

  const valuesArray = rows.map(row => {
    const values = Object.keys(mapping.unidades).map(key => row[key] ?? null);

    // Relacionamentos
    values.push(gruposMap.get(row.ID_GRUPO_UNIDADE) ?? null);     // grupo_unidade_id
    values.push(statusMap.get(row.ID_TIPO_UNIDADE) ?? 1);          // status_id, default 1
    values.push(pessoasMap.get(row.NM_PROPRIETARIO) ?? null);      // prop_pessoa_id

    return values;
  });

  // Inserção em lotes
  for (let i = 0; i < valuesArray.length; i += batchSize) {
    const batch = valuesArray.slice(i, i + batchSize);
    await bulkInsert('unidades', columnsBase, batch);
    console.log(`✅ Batch ${i / batchSize + 1} inserido (${batch.length} unidades).`);
  }

  console.log(`Migração concluída: ${valuesArray.length} unidades inseridas.`);
  client.release();
}

{/* ---------- MIGRAÇÃO DE UNIDADES PESSOAS ---------- */ }
async function migrateUnidadesPessoas(batchSize = 2000) {
  const rows = await getMoradoresFromFirebird();
  const client = await pool.connect();

  try {
    const unidadesMap = new Map();
    const moradoresMap = new Map();

    const unidadesRes = await client.query('SELECT id_outside, id FROM unidades');
    unidadesRes.rows.forEach(r => unidadesMap.set(r.id_outside, r.id));

    const moradoresRes = await client.query(
      "SELECT id_outside, id FROM pessoas WHERE role = 'residente'"
    );
    moradoresRes.rows.forEach(r => moradoresMap.set(r.id_outside, r.id));

    const valuesArray = rows.map(row => {
      const unidadeId = unidadesMap.get(row.ID_UNIDADE);
      const moradorId = moradoresMap.get(row.CD_MORADOR);

      if (!unidadeId || !moradorId) return null;

      return [unidadeId, moradorId];
    }).filter(v => v !== null);

    const columns = ['unidade_id', 'pessoa_id'];

    for (let i = 0; i < valuesArray.length; i += batchSize) {
      const batch = valuesArray.slice(i, i + batchSize);
      // aqui não precisamos capturar insertedIds
      await bulkInsert('unidades_pessoas', columns, batch, null, false);
      console.log(`✅ Batch ${i / batchSize + 1} inserido (${batch.length} registros).`);
    }

    console.log(`Migração concluída: ${valuesArray.length} registros inseridos.`);
  } finally {
    client.release();
  }
}

{/* ---------- MIGRAÇÃO DE VISITANTES ---------- */ }
async function migrateVisitantes() {
  const rows = await getVisitantesFromFirebird();
  const columns = Object.values(mapping.visitante);

  if (!columns.includes('role')) columns.push('role');
  if (!columns.includes('documento')) columns.push('documento');

  // tamanho do lote
  const batchSize = 2000;

  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);

    const valuesArray = batch.map(row => {
      const values = Object.keys(mapping.visitante).map(key => {
        if (key === 'ID_INATIVO') {
          return row[key] === 0; // true se ativo, false se inativo
        }
        return row[key] ?? null;
      });

      values.push('visitante');
      values.push(row.NR_CPF_CPNJ || 'NÃO DISPONÍVEL');
      return values;
    });

    const insertedIds = await bulkInsert('pessoas', columns, valuesArray);
    console.log(`Lote ${i / batchSize + 1}: ${insertedIds.length} visitantes inseridos.`);
  }

  console.log(`Migração concluída: ${rows.length} visitantes inseridos no total.`);
}

{/* ---------- MIGRAÇÃO DE VEÍCULOS MORADORES ---------- */ }
async function migrateVeiculosMoradores(batchSize = 2000) {
  const rows = await getVeiculosMoradores();

  // Conecta no PostgreSQL
  const client = await pool.connect();

  const unidadesMap = new Map();

  // Precarrega unidades para evitar consultas repetidas
  const unidadesRes = await client.query('SELECT id_outside, id FROM unidades');
  unidadesRes.rows.forEach(r => unidadesMap.set(r.id_outside, r.id));

  // Colunas base do mapeamento
  const columnsBase = Object.values(mapping.veiculo);

  // Adiciona colunas extras
  if (!columnsBase.includes('unidade_id')) columnsBase.push('unidade_id');
  if (!columnsBase.includes('tipo')) columnsBase.push('tipo');

  const valuesArray = rows.map(row => {
    // const values = Object.keys(mapping.veiculo).map(key => row[key] ?? null);
    const values = Object.keys(mapping.veiculo).map(key => {
      if (key === 'ID_INATIVO') {
        return row[key] === 0; // true se ativo, false se inativo
      }
      return row[key] ?? null;
    });

    // Unidade
    const unidadeId = unidadesMap.get(row.ID_UNIDADE) ?? null;
    values.push(unidadeId);

    // Tipo de veículo
    const tipoVeiculo = row.ID_TIPO_VEICULO === 'C' ? 'carro' : 'moto';
    values.push(tipoVeiculo);

    return values;
  }).filter(v => v[columnsBase.indexOf('unidade_id')] !== null);

  // Inserção em lotes
  for (let i = 0; i < valuesArray.length; i += batchSize) {
    const batch = valuesArray.slice(i, i + batchSize);
    await bulkInsert('veiculos', columnsBase, batch);
    console.log(`✅ Batch ${i / batchSize + 1} inserido (${batch.length} veículos).`);
  }

  console.log(`Migração concluída: ${valuesArray.length} veículos inseridos.`);
  client.release();
}

{/* ---------- MIGRAÇÃO DE VEÍCULOS VISITANTES ---------- */ }
async function migrateVeiculosVisitantes(batchSize = 2000) {
  const rows = await getVeiculosVisitantes();

  // Conecta no PostgreSQL
  const client = await pool.connect();

  const propsMap = new Map();

  // Precarrega unidades para evitar consultas repetidas
  const propsRes = await client.query(
    "SELECT id_outside, id FROM pessoas WHERE role = 'visitante'"
  );
  propsRes.rows.forEach(r => propsMap.set(r.id_outside, r.id));

  // Colunas base do mapeamento
  const columnsBase = Object.values(mapping.veiculo);

  // Adiciona colunas extras
  if (!columnsBase.includes('pessoa_id')) columnsBase.push('pessoa_id');
  if (!columnsBase.includes('tipo')) columnsBase.push('tipo');

  const valuesArray = rows.map(row => {
    // const values = Object.keys(mapping.veiculo).map(key => row[key] ?? null);
    const values = Object.keys(mapping.veiculo).map(key => {
      if (key === 'ID_INATIVO') {
        return row[key] === 0; // true se ativo, false se inativo
      }
      return row[key] ?? null;
    });

    // Unidade
    const visitanteId = propsMap.get(row.ID_VISITANTE) ?? null;
    values.push(visitanteId);

    // Tipo de veículo
    const tipoVeiculo = row.ID_TIPO_VEICULO === 'C' ? 'carro' : 'moto';
    values.push(tipoVeiculo);

    return values;
  }).filter(v => v[columnsBase.indexOf('pessoa_id')] !== null);

  // Inserção em lotes
  for (let i = 0; i < valuesArray.length; i += batchSize) {
    const batch = valuesArray.slice(i, i + batchSize);
    await bulkInsert('veiculos', columnsBase, batch);
    console.log(`✅ Batch ${i / batchSize + 1} inserido (${batch.length} veículos).`);
  }

  console.log(`Migração concluída: ${valuesArray.length} veículos inseridos.`);
  client.release();
}

{/* ---------- MIGRAÇÃO DE PETS ---------- */ }
async function migratePets(batchSize = 2000) {
  const rows = await getPets();

  const client = await pool.connect();
  const unidadesMap = new Map();

  // Precarregar unidades do PostgreSQL
  const unidadesRes = await client.query('SELECT id_outside, id FROM unidades');
  unidadesRes.rows.forEach(r => unidadesMap.set(r.id_outside, r.id));

  // Colunas base do mapping
  const columnsBase = Object.values(mapping.pets);

  // Adiciona colunas extras se não existirem
  if (!columnsBase.includes('unidade_id')) columnsBase.push('unidade_id');
  if (!columnsBase.includes('especie')) columnsBase.push('especie');
  if (!columnsBase.includes('peso')) columnsBase.push('peso');

  // Prepara os valores para bulk insert
  const valuesArray = rows.map(row => {
    const values = Object.keys(mapping.pets).map(key => row[key] ?? null);

    // Unidade
    const unidadeId = unidadesMap.get(row.ID_UNIDADE) ?? null;
    values.push(unidadeId);

    // Espécie
    const especie = especieMap[row.ID_TIPO] || 'outros';
    values.push(especie);

    // Peso
    const peso = pesoMap[row.ID_PESO] || 'outro';
    values.push(peso);

    return values;
  }).filter(v => v[columnsBase.indexOf('unidade_id')] !== null);

  // Inserção em lotes
  for (let i = 0; i < valuesArray.length; i += batchSize) {
    const batch = valuesArray.slice(i, i + batchSize);
    await bulkInsert('pets', columnsBase, batch);
    console.log(`✅ Batch ${i / batchSize + 1} inserido (${batch.length} pets).`);
  }

  console.log(`Migração concluída: ${valuesArray.length} pets inseridos.`);
  client.release();
}

{/* ---------- MIGRAÇÃO DE OCORRÊNCIAS ---------- */ }
async function migrateOcorrencias(batchSize = 2000) {
  const rows = await getOcorrencias();

  // Colunas base do mapeamento
  const columnsBase = Object.values(mapping.ocorrencia);

  // Monta os valores
  const valuesArray = rows.map(row => {
    const values = Object.keys(mapping.ocorrencia).map(key => row[key] ?? null);
    return values;
  });

  // Inserção em lotes para não travar a memória
  for (let i = 0; i < valuesArray.length; i += batchSize) {
    const batch = valuesArray.slice(i, i + batchSize);
    await bulkInsert('ocorrencias', columnsBase, batch);
    console.log(`✅ Batch ${i / batchSize + 1} inserido (${batch.length} registros).`);
  }

  console.log(`Migração concluída: ${valuesArray.length} ocorrencias inseridas.`);
}

{/* ---------- MIGRAÇÃO DE COMUNICADOS ---------- */ }
async function migrateComunicados(batchSize = 2000) {
  const rows = await getComunicados();

  // Colunas base do mapeamento
  const columnsBase = Object.values(mapping.comunicados);

  // Monta os valores
  const valuesArray = rows.map(row => {
    const values = Object.keys(mapping.comunicados).map(key => row[key] ?? null);
    return values;
  });

  // Inserção em lotes para não travar a memória
  for (let i = 0; i < valuesArray.length; i += batchSize) {
    const batch = valuesArray.slice(i, i + batchSize);
    await bulkInsert('comunicados', columnsBase, batch);
    console.log(`✅ Batch ${i / batchSize + 1} inserido (${batch.length} registros).`);
  }

  console.log(`Migração concluída: ${valuesArray.length} comunicados inseridas.`);
}

{/* ---------- MIGRAÇÃO DE CORRESPONDÊNCIAS ---------- */ }
async function migrateCorrespondencias(batchSize = 2000) {
  const rows = await getCorrespondencias();

  const client = await pool.connect();

  // Colunas base do mapeamento
  const columnsBase = Object.values(mapping.correspondencia);

  // Adiciona colunas extras
  if (!columnsBase.includes('unidade_id')) columnsBase.push('unidade_id');
  if (!columnsBase.includes('pessoa_id_entregue')) columnsBase.push('pessoa_id_entregue');
  if (!columnsBase.includes('entregue')) columnsBase.push('entregue');

  // Pré-carregar unidades e moradores para evitar consultas repetidas
  const unidadesMap = new Map();
  const moradoresMap = new Map();

  const unidadesRes = await client.query('SELECT id_outside, id FROM unidades');
  unidadesRes.rows.forEach(r => unidadesMap.set(r.id_outside, r.id));

  const moradoresRes = await client.query("SELECT id_outside, id FROM pessoas WHERE role = 'residente'");
  moradoresRes.rows.forEach(r => moradoresMap.set(r.id_outside, r.id));

  const valuesArray = rows.map(row => {
    const values = Object.keys(mapping.correspondencia).map(key => row[key] ?? null);

    // Unidade
    const unidadeId = unidadesMap.get(row.ID_UNIDADE) ?? null;
    values.push(unidadeId);

    // Morador entregue
    const moradorId = moradoresMap.get(row.ID_MORADOR_ENTREGUE) ?? null;
    values.push(moradorId);

    // Status de entrega
    const entregueBoolean = row.ID_STATUSO === 1 ? false : true;
    values.push(entregueBoolean);

    return values;
  }).filter(v => v[columnsBase.indexOf('unidade_id')] !== null);

  // Inserção em lotes
  for (let i = 0; i < valuesArray.length; i += batchSize) {
    const batch = valuesArray.slice(i, i + batchSize);
    await bulkInsert('correspondencias', columnsBase, batch);
    console.log(`✅ Batch ${i / batchSize + 1} inserido (${batch.length} correspondências).`);
  }

  console.log(`Migração concluída: ${valuesArray.length} correspondências inseridas.`);
  client.release();
}

{/* ---------- MIGRAÇÃO DE DISPOSITIVOS ---------- */ }
async function migrateDispositivos(batchSize = 2000) {
  const rows = await getDispositivos();

  const client = await pool.connect();

  // Colunas base do mapping
  const columnsBase = Object.values(mapping.dispositivo);

  // Adiciona colunas extras se não existirem
  if (!columnsBase.includes('fabricante')) columnsBase.push('fabricante');
  if (!columnsBase.includes('controle_letra')) columnsBase.push('controle_letra');
  if (!columnsBase.includes('facial_tipos_lib')) columnsBase.push('facial_tipos_lib');

  // Prepara os valores para bulk insert
  const valuesArray = rows
    .filter(row => row.CD_DISPOSITIVO !== 99) // ignora dispositivos com CD_DISPOSITIVO 99 (liberação manual)
    .map(row => {
      const values = Object.keys(mapping.dispositivo).map(key => row[key] ?? null);

      // Fabricante
      const idFabricante = row.ID_FABRICANTE === 8 ? 3 : null; // hikvision
      values.push(idFabricante);

      // Letra de controle
      const letra = letraControle[row.ID_LETRA_CONTROLE] || 'outros';
      values.push(letra);

      // Tipos de liberação
      const tipoLib = facialTipoLib[row.ID_FACIAL_TIPO_LIB_VIS] || null;
      values.push(tipoLib);

      return values;
    });

  // Inserção em lotes
  for (let i = 0; i < valuesArray.length; i += batchSize) {
    const batch = valuesArray.slice(i, i + batchSize);
    await bulkInsert('dispositivos', columnsBase, batch);
    console.log(`✅ Batch ${i / batchSize + 1} inserido (${batch.length} dispositivos).`);
  }

  console.log(`Migração concluída: ${valuesArray.length} dispositivos inseridos.`);
  client.release();
}

{/* ---------- MIGRAÇÃO DE CÂMERAS ---------- */ }
async function migrateCameras() {
  const rows = await getCameras();
  const columns = Object.values(mapping.cameras); // array de colunas

  // Monta os valores
  const valuesArray = rows.map(row => {
    const values = Object.keys(mapping.cameras).map(key => row[key] ?? null);
    return values;
  });

  // Inserir batch no PostgreSQL
  const insertedIds = await bulkInsert('cameras', columns, valuesArray);

  console.log(`Migração concluída: ${insertedIds.length} cameras inseridas.`);
}

async function migrateLiberacoesAcessosTipo() {
  const rows = await getAcessosTipos();
  const columns = Object.values(mapping.liberacoesAcessoTipo); // array de colunas

  // Monta os valores
  const valuesArray = rows.map(row => {
    const values = Object.keys(mapping.liberacoesAcessoTipo).map(key => row[key] ?? null);
    return values;
  });

  // Inserir batch no PostgreSQL
  const insertedIds = await bulkInsert('liberacoes_acessos_tipos', columns, valuesArray);

  console.log(`Migração concluída: ${insertedIds.length} liberacoes_acessos_tipos inseridos.`);
}

{/* ---------- MIGRAÇÃO DE LIBERAÇÕES ---------- */ }
async function migrateLiberacoes(batchSize = 2000) {
  const rows = await getLiberacoes();

  const client = await pool.connect();

  const columnsBase = Object.values(mapping.liberacoes); // array de colunas

  // Adiciona colunas extras
  if (!columnsBase.includes('status')) columnsBase.push('status');
  if (!columnsBase.includes('tipo')) columnsBase.push('tipo');
  if (!columnsBase.includes('dentro')) columnsBase.push('dentro');
  if (!columnsBase.includes('veiculo_id')) columnsBase.push('veiculo_id');
  if (!columnsBase.includes('pessoa_id')) columnsBase.push('pessoa_id');
  if (!columnsBase.includes('acesso_tipo_id')) columnsBase.push('acesso_tipo_id');

  // Pré-carregar relacionamentos para evitar consultas repetidas
  const veiculosRes = await client.query('SELECT id_outside, id FROM veiculos WHERE pessoa_id IS NOT NULL');
  const veiculosMap = new Map(veiculosRes.rows.map(r => [r.id_outside, r.id]));

  const visitantesRes = await client.query("SELECT id_outside, id FROM pessoas WHERE role = 'visitante'");
  const visitantesMap = new Map(visitantesRes.rows.map(r => [r.id_outside, r.id]));

  const tiposAcessoRes = await client.query('SELECT id_outside, id FROM liberacoes_acessos_tipos');
  const tiposAcessoMap = new Map(tiposAcessoRes.rows.map(r => [r.id_outside, r.id]));

  // Prepara os valores para bulk insert
  const valuesArray = rows.map(row => {
    // const values = Object.keys(mapping.liberacoes).map(key => row[key] ?? null);
    const values = Object.keys(mapping.liberacoes).map(key => {
      if (key === 'DT_LIBERADO_ATE') {
        // Ajusta somente se for do tipo "única"
        const tipo = tipoLiberacao[row.ID_TIPO_LIBERACAO];
        if (tipo === 'unica' && row.DT_ENTRADA) {
          const inicio = new Date(row.DT_ENTRADA);
          // força hora 23:59:59
          inicio.setHours(23, 59, 59, 0);
          return inicio.toISOString();
        }
        return row[key] ?? null;
      }

      return row[key] ?? null;
    });

    // Status, tipo, dentro
    values.push(statusLiberacao[row.ID_STATUS] ?? null);
    values.push(tipoLiberacao[row.ID_TIPO_LIBERACAO] ?? null);
    values.push(dentroMap[row.ID_STATUS] ?? null);

    // Veículo
    values.push(veiculosMap.get(row.ID_VEICULO_UTILIZADO) ?? null);

    // Visitante
    values.push(visitantesMap.get(row.ID_PRESTADOR) ?? null);

    // Tipo de acesso
    values.push(tiposAcessoMap.get(row.ID_TIPO_ACESSO) ?? null);

    return values;
  });

  // Inserção em lotes
  for (let i = 0; i < valuesArray.length; i += batchSize) {
    const batch = valuesArray.slice(i, i + batchSize);
    await bulkInsert('liberacoes', columnsBase, batch);
    console.log(`✅ Batch ${i / batchSize + 1} inserido (${batch.length} liberações).`);
  }

  console.log(`Migração concluída: ${valuesArray.length} liberações inseridas.`);
  client.release();
}

{/* ---------- MIGRAÇÃO DE LIBERAÇÕES UNIDADES ---------- */ }
async function migrateLiberacoesUnidades(batchSize = 2000) {
  const rows = await getLiberacoes();
  const client = await pool.connect();

  // Normaliza colunas do mapeamento
  const columnsBase = Object.values(mapping.liberacoesUnidades).map(c => c.trim());

  // Adiciona colunas extras
  if (!columnsBase.includes('liberacao_id')) columnsBase.push('liberacao_id');
  if (!columnsBase.includes('unidade_id')) columnsBase.push('unidade_id');
  if (!columnsBase.includes('solicitante_id')) columnsBase.push('solicitante_id');

  // Pré-carregar relacionamentos
  const liberacoesRes = await client.query('SELECT id_outside, id FROM liberacoes');
  const liberacoesMap = new Map(liberacoesRes.rows.map(r => [r.id_outside, r.id]));

  const pessoasRes = await client.query(`SELECT id_outside, id FROM pessoas WHERE role = 'residente'`);
  const pessoasMap = new Map(pessoasRes.rows.map(r => [r.id_outside, r.id]));

  const unidadesPessoasRes = await client.query(
    `SELECT up.pessoa_id, up.unidade_id 
     FROM unidades_pessoas up 
     INNER JOIN pessoas p ON up.pessoa_id = p.id`
  );
  const unidadesMap = new Map(unidadesPessoasRes.rows.map(r => [r.pessoa_id, r.unidade_id]));

  // Função para normalizar valores antes de inserir
  function normalizeValue(colName, value) {
    if (value == null) return null;
    const col = colName.trim().toLowerCase();

    // Campos do tipo time
    if (col.includes('hr_')) {
      const d = new Date(value);
      if (isNaN(d)) return null;
      return d.toTimeString().split(' ')[0]; // "HH:MM:SS"
    }

    return value;
  }

  // Prepara os valores para bulk insert
  const valuesArray = rows.map(row => {
    const values = Object.keys(mapping.liberacoesUnidades).map(
      key => normalizeValue(mapping.liberacoesUnidades[key], row[key]) ?? null
    );

    const liberacaoId = liberacoesMap.get(row.CD_ACESSO) ?? null;
    const solicitanteId = pessoasMap.get(row.ID_MORADOR) ?? null;
    const unidadeId = unidadesMap.get(solicitanteId) ?? null;

    values.push(liberacaoId, unidadeId, solicitanteId);

    return values;
  }).filter(v => v[columnsBase.indexOf('liberacao_id')] !== null);

  // Inserção em lotes
  for (let i = 0; i < valuesArray.length; i += batchSize) {
    const batch = valuesArray.slice(i, i + batchSize);
    await bulkInsert('liberacoes_unidades', columnsBase, batch);
    console.log(`✅ Batch ${i / batchSize + 1} inserido (${batch.length} liberações de unidades).`);
  }

  console.log(`Migração concluída: ${valuesArray.length} liberações de unidades inseridas.`);
  client.release();
}

// async function updateLiberacoesUnidadesFromFirebird(batchSize = 2000) {
//   const client = await pool.connect();

//   try {
//     // Consulta os dados diretamente do Firebird
//     const rows = await getLiberacoes();

//     // Mapas de relacionamento no PostgreSQL
//     const liberacoesRes = await client.query('SELECT id_outside, id FROM liberacoes');
//     const liberacoesMap = new Map(liberacoesRes.rows.map(r => [r.id_outside, r.id]));

//     const pessoasRes = await client.query(`SELECT id_outside, id FROM pessoas WHERE role = 'residente'`);
//     const pessoasMap = new Map(pessoasRes.rows.map(r => [r.id_outside, r.id]));

//     const unidadesPessoasRes = await client.query(
//       `SELECT up.pessoa_id, up.unidade_id 
//        FROM unidades_pessoas up 
//        INNER JOIN pessoas p ON up.pessoa_id = p.id`
//     );
//     const unidadesMap = new Map(unidadesPessoasRes.rows.map(r => [r.pessoa_id, r.unidade_id]));

//     // Atualização em batch
//     for (let i = 0; i < rows.length; i += batchSize) {
//       const batch = rows.slice(i, i + batchSize);

//       for (const row of batch) {
//         const liberacaoId = liberacoesMap.get(row.CD_ACESSO) ?? null;
//         const solicitanteId = pessoasMap.get(row.ID_MORADOR) ?? null;
//         const unidadeId = unidadesMap.get(solicitanteId) ?? null;

//         if (!liberacaoId) continue; // só atualiza se existir a liberacao correspondente

//         await client.query(
//           `UPDATE liberacoes_unidades
//            SET solicitante_id = $1, unidade_id = $2
//            WHERE liberacao_id = $3`,
//           [solicitanteId, unidadeId, liberacaoId]
//         );
//       }

//       console.log(`✅ Batch ${i / batchSize + 1} atualizado (${batch.length} registros).`);
//     }

//     console.log('Atualização completa com dados do Firebird.');
//   } finally {
//     client.release();
//   }
// }

{/* ---------- MIGRAÇÃO DE EVENTOS ---------- */ }
async function migrateEventos() {
  const rows = await getEventosFromFirebird();
  const columnsBase = Object.values(mapping.eventos); // colunas mapeadas do Firebird

  // Adiciona colunas que não vêm do Firebird
  const extraColumns = ['pessoa_id', 'liberacao_id', 'dispositivo_id', 'direcao'];
  const columns = [...columnsBase, ...extraColumns];

  const BATCH_SIZE = 1000;

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);

    const valuesArray = await Promise.all(batch.map(async (row) => {
      const values = Object.keys(mapping.eventos).map(key => row[key] ?? null);

      // pessoa_id
      let pessoaId = null;
      if (row.ID_PESSOA) {
        const role = row.ID_TIPO_PESSOA === 0 ? 'residente' : 'visitante';
        const res = await pool.query(
          'SELECT id FROM pessoas WHERE id_outside = $1 AND role = $2',
          [row.ID_PESSOA, role]
        );
        pessoaId = res.rows[0]?.id || null;
      }
      values.push(pessoaId);

      // liberacao_id
      let liberacaoId = null;
      if (row.ID_ACESSO) {
        const res = await pool.query(
          'SELECT id FROM liberacoes WHERE id_outside = $1',
          [row.ID_ACESSO]
        );
        liberacaoId = res.rows[0]?.id || null;
      }
      values.push(liberacaoId);

      // dispositivo_id
      let dispositivoId = null;
      if (row.ID_DISPOSITIVO) {
        const res = await pool.query(
          'SELECT id FROM dispositivos WHERE id_outside = $1',
          [row.ID_DISPOSITIVO]
        );
        dispositivoId = res.rows[0]?.id || null;
      }
      values.push(dispositivoId);

      // direcao
      const direcao = direcaoMap[row.ID_DIRECAO] || 'indisponivel';
      values.push(direcao);

      return values;
    }));

    // Inserir batch no PostgreSQL
    const insertedIds = await bulkInsert('eventos', columns, valuesArray);
    console.log(`✅ Batch ${i / BATCH_SIZE + 1} inserido (${insertedIds.length} registros)`);
  }

  console.log('Migração de eventos concluída.');
}

// async function updateDataFimLiberacoes() {
//   return new Promise((resolve, reject) => {
//     Firebird.attach(firebirdConfig, async (err, firebirdClient) => {
//       if (err) return reject(`Erro ao conectar ao Firebird: ${err}`);

//       firebirdClient.query(
//         'SELECT * FROM TAB_ACESSO_PRESTADOR where ID_TIPO_LIBERACAO = 1',
//         async (err, result) => {
//           if (err) {
//             firebirdClient.detach();
//             return reject(`Erro ao consultar TAB_ACESSO_PRESTADOR: ${err}`);
//           }

//           const client = await pool.connect();
//           try {
//             for (const row of result) {

//               const liberacaoQuery = `SELECT id FROM liberacoes WHERE id_outside = $1'`;
//               const liberacaoResult = await client.query(liberacaoQuery, [row.CD_ACESSO]);

//               if (liberacaoResult.rows.length === 0) {
//                 continue;
//               }

//               const liberacaoId = liberacaoResult.rows[0].id;

//               const query = `
//                   UPDATE liberacoes set data_fim = $1 where id_outside = $2;
//                 `;

//               await client.query(query, [row.DT_ENTRADA, liberacaoId]);
//             }

//             console.log('liberações data_fim alteradas com sucesso.');
//             resolve();
//           } catch (error) {
//             reject(`Erro ao alterar liberações data_fim: ${error}`);
//           } finally {
//             client.release();
//             firebirdClient.detach();
//           }
//         }
//       );
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

    // console.log('Iniciando o update na tabela liberacoes_unidades');
    // await updateLiberacoesUnidadesFromFirebird();

    console.log('Migração concluída com sucesso!');
  } catch (error) {
    console.error('Erro durante a migração:', error);
  } finally {
    await pool.end(); // Encerra a conexão com o PostgreSQL
  }
}

// Executa a migração
migrateAllTables();
