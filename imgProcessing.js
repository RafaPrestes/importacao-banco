const jimp = require('jimp');
const fs = require('fs');
const axios = require('axios');
const FormData = require('form-data');

class ImageProcessing {
  async base64ToJPEG(base64String) {
    // Remove a parte "data:image/jpeg;base64," se existir
    const base64Data = base64String.split(';base64,').pop();

    // Converte o base64 em um buffer
    const buffer = Buffer.from(base64Data, 'base64');

    // Salva em um arquivo temporário (opcional)
    const filePath = './temp_image.jpeg';
    fs.writeFileSync(filePath, buffer);

    return filePath;
  }

  async sendImageToServer(imagePath) {
    const form = new FormData();
    form.append('file', fs.createReadStream(imagePath));

    try {
      const response = await axios.post('http://192.168.0.12:3337/arquivos', form, {
        headers: {
          ...form.getHeaders(),
        },
      });

      const fileId = response.data.id;
      console.log('Imagem enviada com sucesso, ID do arquivo:', fileId);

      return fileId;
    } catch (error) {
      console.error('Erro ao enviar imagem:', error);
    }
  }

  async base64ToBuffer(base64) {
    const bufOrigin = Buffer.from(base64, 'base64');

    // Converte para .BMP
    let bmpBase64;
    await new Promise((resolve, reject) => {
      jimp.read(bufOrigin, (err, image) => {
        if (err) throw err;
        else {
          image
            .resize(640, jimp.AUTO)
            .quality(60)
            .getBase64(jimp.MIME_JPEG, (error, src) => {
              resolve((bmpBase64 = src.split(';base64,').pop()));
            });
        }
      });
    });

    const bmpBuffer = Buffer.from(bmpBase64, 'base64');

    return bmpBuffer;
  }

  async bufferToBase64(field) {
    let img;

    await new Promise((resolve, reject) => {
      field((err, name, eventEmitter) => {
        const buffers = [];
        eventEmitter.on('data', (chunk) => {
          buffers.push(chunk);
        });
        eventEmitter.once('end', async () => {
          const buffer = Buffer.concat(buffers);
          img = buffer.toString('base64');

          resolve(img);
        });
      });
    });

    return img;
  }

  async resize(base64) {
    let img;
    const buffer = Buffer.from(base64, 'base64');

    await new Promise((resolve, reject) => {
      jimp.read(buffer, (err, image) => {
        if (err) throw err;
        else {
          image
            .resize(350, jimp.AUTO)
            .quality(50)
            .getBase64(jimp.MIME_JPEG, (err, src) => {
              resolve((img = src.split(';base64,').pop()));
            });
        }
      });
    });
    return img;
  }

  async imgToBase64(field) {
    if (!field) return null;

    const originalImg = await this.bufferToBase64(field);
    const resizedImg = await this.resize(originalImg);

    return resizedImg;
  }
}

// Exporte a classe usando o padrão CommonJS
module.exports = new ImageProcessing();
