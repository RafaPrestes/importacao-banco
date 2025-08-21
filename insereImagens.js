const Firebird = require('node-firebird');
const ImageProcessing = require('./imgProcessing');
const { pool } = require('./db.js');
const { firebirdConfig } = require('./db.js');

async function enviarFotosPessoas(tabela, role, idColumn, campoImagem, filtroExtra = null) {
  const pgClient = await pool.connect();

  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, db) => {
      if (err) {
        console.error('Erro ao conectar ao Firebird:', err);
        return;
      }

      // Sempre filtra por imagem não nula
      let queryFirebird = `SELECT * FROM ${tabela} WHERE ${campoImagem} IS NOT NULL`;
      if (filtroExtra) {
        queryFirebird += ` AND ${filtroExtra}`;
      }

      db.query(queryFirebird, async (err, pessoaFirebird) => {
        if (err) {
          console.error(`Erro ao consultar ${tabela}:`, err);
          db.detach();
          return;
        }

        try {
          const { rows: pessoasNuvem } = await pgClient.query(`
          SELECT id_outside, role
          FROM pessoas 
          WHERE role = $1
            AND (foto_face_id IS NULL OR foto_documento_id IS NULL)
        `, [role]);

          // Cria um Map usando id_outside + role para garantir que só atualize o role correto
          const pessoasNuvemMap = new Map(
            pessoasNuvem.map(p => [`${p.id_outside}_${p.role}`, p])
          );

          const pessoasParaAtualizar = pessoaFirebird.filter(row =>
            pessoasNuvemMap.has(`${row[idColumn]}_${role}`)
          );

          console.log(`Encontrados ${pessoasParaAtualizar.length} ${role}s para envio de fotos.`);

          for (let i = 0; i < pessoasParaAtualizar.length; i++) {
            const row = pessoasParaAtualizar[i];
            console.log(`[${i + 1}/${pessoasParaAtualizar.length}] Processando ${role} ${row[idColumn]}...`);

            const updates = [];

            const base64Face = await ImageProcessing.imgToBase64(row[campoImagem]);
            const base64Doc = await ImageProcessing.imgToBase64(row.IMG_DOCUMENTO);

            if (base64Face) {
              const pathFace = await ImageProcessing.base64ToJPEG(base64Face);
              const fotoFaceId = await ImageProcessing.sendImageToServer(pathFace);
              updates.push({ column: 'foto_face_id', value: fotoFaceId });
            }

            if (base64Doc) {
              const pathDoc = await ImageProcessing.base64ToJPEG(base64Doc);
              const docImgId = await ImageProcessing.sendImageToServer(pathDoc);
              updates.push({ column: 'foto_documento_id', value: docImgId });
            }

            if (updates.length > 0) {
              const setClause = updates.map((u, idx) => `${u.column} = $${idx + 1}`).join(', ');
              const values = updates.map(u => u.value);
              values.push(row[idColumn]);

              await pgClient.query(
                `UPDATE pessoas SET ${setClause} WHERE id_outside = $${updates.length + 1} AND role = $${updates.length + 2}`,
                [...values, role]
              );

              console.log(`   - ${role} ${row[idColumn]} atualizado com fotos.`);
            }
          }

          console.log(`Envio de fotos de ${role} concluído!`);
          resolve();
        } catch (err) {
          console.error('Erro ao enviar fotos:', err);
          reject(err);
        } finally {
          db.detach();
          pgClient.release();
        }
      });
    });
  })
}

async function enviarFotos(tabelaFirebird, tabelaPostgres, idColumn, campoImagemPostgres, campoImagemFirebird, filtroExtra = null) {
  const pgClient = await pool.connect();

  return new Promise((resolve, reject) => {
    Firebird.attach(firebirdConfig, async (err, db) => {
      if (err) {
        console.error('Erro ao conectar ao Firebird:', err);
        return;
      }

      db.query(`SELECT * FROM ${tabelaFirebird}`, async (err, itemFirebird) => {
        if (err) {
          console.error(`Erro ao consultar ${tabelaFirebird}:`, err);
          db.detach();
          return;
        }

        try {
          let query = `SELECT id_outside FROM ${tabelaPostgres} WHERE ${campoImagemPostgres} IS NULL`;
          if (filtroExtra) {
            query += ` AND ${filtroExtra}`;
          }

          const { rows: ItensNuvem } = await pgClient.query(query);
          const idsNuvem = new Set(ItensNuvem.map(v => v.id_outside));

          const ItensParaAtualizar = itemFirebird.filter(v => idsNuvem.has(v[idColumn]));

          console.log(`Encontrados ${ItensParaAtualizar.length} ${tabelaPostgres} para envio de fotos.`);

          for (let i = 0; i < ItensParaAtualizar.length; i++) {
            const row = ItensParaAtualizar[i];
            console.log(`[${i + 1}/${ItensParaAtualizar.length}] Processando ${tabelaPostgres} ${row[idColumn]}...`);

            const updates = [];

            console.log(`  - Convertendo imagem pra base64 do ${tabelaPostgres} de id ${row[idColumn]}...`);
            const base64Face = await ImageProcessing.imgToBase64(row[campoImagemFirebird]);

            if (base64Face) {
              console.log(`  - enviando para o servidor a foto do ${tabelaPostgres} de id ${row[idColumn]}...`);
              const pathFace = await ImageProcessing.base64ToJPEG(base64Face);
              const fotoFaceId = await ImageProcessing.sendImageToServer(pathFace);
              updates.push({ column: 'foto_id', value: fotoFaceId });
              console.log(`  ✔ Foto enviada com ID ${fotoFaceId}`);
            }

            // Atualiza na nuvem
            if (updates.length > 0) {
              const setClause = updates.map((u, idx) => `${u.column} = $${idx + 1}`).join(', ');
              const values = updates.map(u => u.value);
              let whereClause = `id_outside = $${updates.length + 1}`;
              values.push(row[idColumn]);

              if (filtroExtra) {
                whereClause += ` AND ${filtroExtra}`;
              }

              await pgClient.query(
                `UPDATE ${tabelaPostgres} SET ${setClause} WHERE ${whereClause}`,
                values
              );

              console.log(`   - ${tabelaPostgres} ${row[idColumn]} atualizado com fotos.`);
            }
          }

          console.log('Envio de fotos concluído!');
          resolve();
        } catch (err) {
          console.error('Erro ao enviar fotos:', err);
          reject(err);
        } finally {
          db.detach();        // detacha Firebird
          pgClient.release(); // libera Postgres
        }
      });
    });
  })
}

async function enviarFotosVisitantes() {
  enviarFotosPessoas('TAB_PRESTADOR', 'visitante', 'CD_PRESTADOR', 'IMG_FACE');
}

async function enviarFotosMoradores() {
  enviarFotosPessoas('TAB_MORADOR', 'residente', 'CD_MORADOR', 'IMG_MORADOR', 'ID_TIPO_MORADOR <> 99');
}

async function enviarFotosPets() {
  enviarFotos('TAB_ANIMAL_DOMESTICO', 'pets', 'CD_ANIMAL', 'foto_id', 'IMG_ANIMAL');
}

async function enviarFotosVeiculoMorador() {
  enviarFotos('TAB_VEICULO_MORADOR', 'veiculos', 'CD_VEICULO', 'foto_id', 'IMG_VEICULO', 'unidade_id IS NOT NULL');
}

async function enviarFotosVeiculoVisitante() {
  enviarFotos('TAB_VEICULO_VISITANTE', 'veiculos', 'CD_VEICULO', 'foto_id', 'IMG_VEICULO', 'pessoa_id IS NOT NULL');
}

async function migrateFotos() {
  // console.log('Iniciando migração de fotos moradores');
  // await enviarFotosMoradores();

  // console.log('Iniciando migração de fotos visitantes');
  // await enviarFotosVisitantes();

  // console.log('Iniciando migração de fotos veiculos morador');
  // await enviarFotosVeiculoMorador();

  // console.log('Iniciando migração de fotos veiculos visitante');
  // await enviarFotosVeiculoVisitante();

  // console.log('Iniciando migração de fotos pets');
  // await enviarFotosPets();

}

migrateFotos();
