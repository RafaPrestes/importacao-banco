const { pool } = require('./db.js');

/**
 * Insere dados no Postgres de forma genérica e rápida.
 * @param {string} table - Nome da tabela.
 * @param {string[]} columns - Array de colunas do banco (sem created_at e updated_at).
 * @param {any[][]} valuesArray - Array de arrays de valores. Cada subarray é uma linha.
 * @param {string|null} conflictColumn - Coluna para ON CONFLICT (opcional).
 * @param {boolean} hasId - Se a tabela tem coluna "id" para retornar.
 */
async function bulkInsert(table, columns, valuesArray, conflictColumn = null, hasId = true) {
  if (!valuesArray || valuesArray.length === 0) return [];

  const client = await pool.connect();
  try {
    // placeholders para cada linha
    const placeholders = valuesArray
      .map(
        (_, i) => `(${columns.map((_, j) => `$${i * columns.length + j + 1}`).join(', ')}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
      )
      .join(', ');

    const flattenedValues = valuesArray.flat();

    let query = `
      INSERT INTO ${table} (${columns.join(',')}, created_at, updated_at)
      VALUES ${placeholders}
    `;

    if (conflictColumn) {
      query += `
        ON CONFLICT (${conflictColumn})
        DO UPDATE SET ${columns.map(col => `${col} = EXCLUDED.${col}`).join(', ')}, updated_at = CURRENT_TIMESTAMP
      `;
    }

    if (hasId) {
      query += ' RETURNING id;';
      const result = await client.query(query, flattenedValues);
      return result.rows.map(row => row.id);
    } else {
      await client.query(query, flattenedValues);
      return []; // não retorna nada se não houver id
    }
  } catch (err) {
    console.error(`Erro ao inserir em ${table}:`, err);
    return [];
  } finally {
    client.release();
  }
}

module.exports = { bulkInsert };
