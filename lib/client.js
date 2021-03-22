/**
 * sqlite3 client
 */

// const util = require('util');
const path = require('path');
const fs = require('fs');

let sqlite3;

module.exports = {
  pool: null,
  init() {
    try {
      sqlite3 = require('sqlite3').verbose();
    } catch (err) {
      return err; // Не установлен npm модуль - больше не перезагружать
    }
  },

  async createPoolToDatabase(dbopt) {
    const folder = path.dirname(dbopt.dbPath);
    if (!fs.existsSync(folder)) {
      fs.mkdirSync(folder, { recursive: true });
    }
    this.pool = await this.connect(dbopt.dbPath);
  },

  connect(dbPath) {
    return new Promise((resolve, reject) => {
      let db = new sqlite3.Database(dbPath, err => {
        if (err) {
          reject(err);
        } else {
          resolve(db);
        }
      });
    });
  },

  run(sql) {
    return new Promise((resolve, reject) => {
      this.pool.run(sql, function(err) {
        // Не исп стрелочную ф-ю, т к callback run возвращает this
        if (!err) {
          resolve(this.changes);
        } else reject(err);
      });
    });
  },

  query(sql) {
    return new Promise((resolve, reject) => {
      this.pool.all(sql, (err, records) => {
        if (!err) {
          resolve(records);
        } else reject(err);
      });
    });
  },

  createTable(query, tableName) {
    return new Promise((resolve, reject) => {
      this.pool.all(`SELECT name FROM sqlite_master WHERE type='table' AND name='${tableName}'`, (e, table) => {
        if (table.length == 1) {
          resolve();
        } else {
          this.pool.run(query, err => {
            if (!err) {
              resolve();
            } else reject(err);
          });
        }
      });
    });
  }
};
