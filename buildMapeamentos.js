// utils.js
const mapeamentos = require('./mapeamentos');

const { booleanColumnsTrue, booleanColumnsFalse } = require('./booleans');

function formatTime(value) {
  if (!value) return null; 

  // Caso o valor já esteja no formato "HH:MM:SS", retorna diretamente
  if (typeof value === 'string' && value.match(/^\d{2}:\d{2}:\d{2}$/)) {
    return value;
  }
  const time = new Date(value);

  const offsetInHours = -3; // Ajuste para UTC-3
  time.setHours(time.getHours() + offsetInHours);

  // Retorna o horário formatado no formato "HH:MM:SS"
  return time.toISOString().substring(11, 19);
}

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
    let value = row[sourceKey]; // Valor do campo no JSON

    // Converte os campos do tipo TIME removendo a data inicial
    if (targetColumn.includes('_hr_inicio') || targetColumn.includes('_hr_fim')) {
      value = formatTime(value);
    }

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