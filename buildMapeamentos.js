// utils.js
const mapeamentos = require('./mapeamentos');

const { booleanColumnsTrue, booleanColumnsFalse } = require('./booleans');

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
    if (booleanColumnsTrue.includes(targetColumn)) {
      values.push(value === 1);
    } else if (booleanColumnsFalse.includes(targetColumn)) {
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