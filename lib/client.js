/**
 * sqlite3 client
 */
const path = require('path');
const fs = require('fs');

let sqlite3;


module.exports = {
  pool:null,
  init() {
    try {
      sqlite3 = require('sqlite3').verbose();
    } catch (err) {
      return err; // Не установлен npm модуль - больше не перезагружать
    }
  },

  async createPoolToDatabase(dbopt, logger) {
    const database = dbopt.dbPath;
    console.log(database);
    this.pool = await this.connect(dbopt,logger);
  },

  connect(dbopt, logger) {
    return new Promise((resolve, reject) => {
      
      //const dbPath = path.join(__dirname, dbopt.dbPath);
      fs.mkdirSync(path.dirname(dbopt.dbPath), {recursive:true});
      
      let db = new sqlite3.Database(dbopt.dbPath, (err) => {
        if (err) {
          reject(err);
        } else {
          db.serialize(() => {
            db.run( 'PRAGMA journal_mode = WAL;' );
            db.run( 'PRAGMA synchronous = NORMAL;' );
            db.run('CREATE INDEX IF NOT EXISTS idx_records_ts ON records (ts);');
          });

          resolve(db);
        }
      });
    });
  },


  writePoints(tableName, columns, values, logger) {
    
    let query = 'INSERT INTO ' + tableName + ' (' + columns.join(',') + ') VALUES ' ;
    values1 = values.map(i => `(${i})`).join(', ');
    //let sql = query + '('+ values1 + ')';
    let sql = query + ' '+ values1;
    //let values1 = values.map((value) => '(?)').join(',');
    //let sql = 'INSERT INTO records (dn,prop,ts,val) VALUES ("DT101","value",1608734670896,20)'
    //logger.log('Sql ' + values1);
    
    return new Promise((resolve, reject) => {
      this.pool.run(sql, err => {
      //  this.pool.query(query, err => {
        if (!err) {
          resolve();
        } else reject(err);
      });
    });
  },


  query(query) {
    return new Promise((resolve, reject) => {
      this.pool.all(query, (err, records) => {
        if (!err) {
          resolve(records);
        } else reject(err);
      });
    });
  },

  createTable(query, tableName) {
    return new Promise((resolve, reject) => {
      this.pool.all(`SELECT name FROM sqlite_master WHERE type='table' AND name='${tableName}'`, (err, table) => {
        if (table.length == 1) {
          resolve();
        } else {
          this.pool.run(query, (err, records) => {
          if (!err) {
            resolve();
          } else reject(err);
          });
        }
    });  
  });
  },


};


// Частные функции
