// utils.js
const mapeamentos = require('./mapeamentos');

const booleanColumns1True = [
  'recebe_push_noti',
  'acesso_academia',
  'app_utiliza',
  'app_cadastra_residente',
  'app_edita_residente',
  'app_cadastra_veiculo',
  'app_edita_veiculo',
  'app_cadastra_pet',
  'app_edita_pet',
  'permitido_grupo_pessoas',
  'permitido_bicileta',
  'autorizado_facial',
  'autorizado_qr',
  'requer_revista',
  'liberado',
  'controle_letra_a',
  'controle_letra_b',
  'controle_letra_c',
  'controle_letra_d'
];

const booleanColumns1False = [
  'ativo',
];

// Função para construir dinamicamente columns e values
function buildColumnsAndValues(row, tipo) {
  const mapping = mapeamentos[tipo]; // Seleciona o mapeamento pelo tipo
  if (!mapping) {
    throw new Error(`Tipo de mapeamento desconhecido: ${tipo}`);
  }

  const columns = [];
  const values = [];

  for (const [sourceKey, targetColumn] of Object.entries(mapping)) {
    columns.push(targetColumn); // Adiciona a coluna do banco
    const value = row[sourceKey]; // Valor do campo no JSON

    // Verifica se a coluna é uma coluna booleana
    if (booleanColumns1True.includes(targetColumn)) {
      values.push(value === 1);
    } else if (booleanColumns1False.includes(targetColumn)) {
      values.push(value === 0);
    } else if (targetColumn === 'documento') {
      values.push(value || 'N/A');
    } else if (targetColumn === 'dt_nascimento') {
      values.push(value || null);
    } else {
      values.push(value || null);
    }
  }

  return { columns, values };
}

module.exports = { buildColumnsAndValues };