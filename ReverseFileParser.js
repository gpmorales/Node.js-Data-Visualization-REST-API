const { Transform } = require('stream');
const CSV = require('csv-parser');

class ReverseCSVParser extends Transform {
  constructor(headers) {
    super({ objectMode: true });
    this.headers = headers;
    this.parser = CSV({ columns: this.headers.reverse(), skip_empty_lines: true });
    this.data = [];
  }

  _transform(chunk, encoding, callback) {
    this.data.unshift(chunk);
    callback();
  }

  _flush(callback) {
    const reversedData = this.data.reverse();
    for (const chunk of reversedData) {
      this.parser.write(chunk);
    }
    this.parser.end();

    let records = [];

    this.parser.on('readable', () => {
      let record;
      while ((record = this.parser.read())) {
        records.push(record);
      }
    });

    this.parser.on('error', (err) => {
      callback(err);
    });

    this.parser.on('end', () => {
      for (let i = records.length - 1; i >= 0; i--) {
        this.push(records[i]);
      }
      callback();
    });
  }
}

module.exports = ReverseCSVParser;
