/**
 * dbagent - client for Sqlite3
 */
const util = require('util');
const schedule = require('node-schedule');
const { promises: fs } = require('fs');
const client = require('./client');
const utils = require('./utils');

module.exports = async function(channel, opt, logger) {
  const initErr = client.init();
  if (initErr) processExit(0, initErr); // Модуль sqlite3 не установлен

  const options = getOptions(opt); // archDays

  let hoursRule = new schedule.RecurrenceRule();
  // hoursRule.rule = '*/15 * * * * *';
  hoursRule.rule = '0 0 * * * *';

  let j = schedule.scheduleJob(hoursRule, () => {
    send({ id: 'settings', type: 'settings' });
    /* channel.on('message', ({ id, type, query, payload }) => {
      if (type == 'settings') return del(payload);
    });*/
    // del(options); //Deleting rows according to the number of days of storage
  });

  logger.log('Options: ' + JSON.stringify(options), 2);

  try {
    await client.createPoolToDatabase(options, logger);
    if (!client.pool) throw { message: 'Client creation Failed!' };

    /*
    db.serialize(() => {
      db.run('PRAGMA journal_mode = WAL;');
      db.run('PRAGMA synchronous = NORMAL;');
      db.run('CREATE INDEX IF NOT EXISTS idx_records_ts ON records (ts);');
    });
    */
   
    await client.run('PRAGMA journal_mode = WAL;');
    await client.run('PRAGMA synchronous = NORMAL;');

    await client.createTable(getCreateTableStr('records'), 'records');
    await client.run('CREATE INDEX IF NOT EXISTS idx_records_ts ON records (ts);');

    channel.on('message', ({ id, type, query, payload }) => {
      if (type == 'write') return write(id, payload);
      if (type == 'read') return read(id, query);
      if (type == 'settings') return del(payload);
    });

    process.on('SIGTERM', () => {
      logger.log('Received SIGTERM');
      processExit(0);
    });

    process.on('exit', () => {
      if (client && client.pool) client.pool.close();
    });
  } catch (err) {
    processExit(1, err);
  }

  /**
   *
   * @param {String} id - request uuid
   * @param {Array of Objects} payload - [{ dn, prop, ts, val }]
   */
  async function write(id, payload) {
    const columns = ['dn', 'prop', 'ts', 'val'];
    const values = utils.formValues(payload, columns);
    if (!values || !values.length) return;

    const tableName = 'records';
    const query = 'INSERT INTO ' + tableName + ' (' + columns.join(',') + ') VALUES ';
    const values1 = values.map(i => `(${i})`).join(', ');
    // let sql = query + '('+ values1 + ')';
    let sql = query + ' ' + values1;
    // let values1 = values.map((value) => '(?)').join(',');
    // let sql = 'INSERT INTO records (dn,prop,ts,val) VALUES ("DT101","value",1608734670896,20)'
    // logger.log('Sql ' + values1);
    try {
      await client.run(sql);
      logger.log('Write query id=' + id + util.inspect(payload), 2);
    } catch (err) {
      sendError(id, err);
    }
  }

  async function del(options) {
    let archDays = [1, 7, 15, 30, 90, 180, 360, 500];

    let tableName = 'records';
    for (const archDay of archDays) {
      let arrDnProp = options.rp.filter(object => object.days == archDay);
      await deletePoints(tableName, archDay, arrDnProp);
    }

    /* logger.log('Row(s) deleted', 1);
    let result = await client.deletePoints(tableName, archDay, arrDnProp , logger);*/

    /* let stats = await fs.stat(options.dbPath);
    let fileSize = stats["size"]/1048576;
    logger.log(`Row(s) deleted ${result}, Dbsize= ${fileSize.toFixed(2)} Mb`, 1);*/
  }

  async function deletePoints(tableName, archDay, arrDnProp) {
    logger.log('Archday=' + archDay + ' ArrayofProps=' + JSON.stringify(arrDnProp), 1);
    let archDepth = archDay * 86400000;
    let delTime = Date.now() - archDepth;
    if (!arrDnProp.length) return;

    let values = arrDnProp.map(i => `(dn='${i.dn}' AND prop='${i.prop}')`).join(' OR ');
    logger.log('Map=' + values, 1);
    let sql = `DELETE FROM ${tableName} WHERE (${values}) AND ts<${delTime}`;
    try {
      const changes = await client.run(sql);
      logger.log(`Row(s) deleted ${changes}`, 1);
    } catch (err) {
      sendError('delete', err);
    }
  }

  async function read(id, queryObj) {
    let firstTime, secondTime, diffTime;
    logger.log('Read query id=' + id + util.inspect(queryObj), 1);
    const dnarr = queryObj.dn_prop.split(',');
    const queryStr = utils.getQueryStr(queryObj, dnarr);
    logger.log('SQL: ' + queryStr, 1);
    firstTime = Date.now();

    try {
      const result = await client.query(queryStr);
      secondTime = Date.now();
      diffTime = secondTime - firstTime;
      logger.log('Get result ' + id, 2);
      logger.log('result length = ' + result.length + ', requestTime: ' + diffTime + 'ms', 1);
      const payload = queryObj.target == 'trend' ? formForTrend(result) : result;
      send({ id, query: queryObj, payload });
    } catch (err) {
      sendError(id, err);
    }

    function formForTrend(res) {
      return dnarr.length == 1 ? res.map(item => [item.ts, Number(item.val)]) : utils.recordsForTrend(res, dnarr);
    }
  }

  function settings(id, query, payload) {
    logger.log('Recieve settings' + JSON.stringify(payload), 1);
    // if (query.loglevel) logger.setLoglevel(query.loglevel);
  }

  function send(message) {
    if (channel.connected) channel.send(message);
  }

  function sendError(id, err) {
    logger.log(err);
    send({ id, error: utils.getShortErrStr(err) });
  }

  function getOptions(argOpt) {
    //
    const res = {};

    return Object.assign(res, argOpt);
  }

  function processExit(code, err) {
    let msg = '';
    if (err) msg = 'ERROR: ' + utils.getShortErrStr(err) + ' ';

    if (client && client.pool) {
      client.pool.close();
      client.pool = null;
      msg += 'Close connection pool.';
    }

    logger.log(msg + ' Exit with code: ' + code);
    setTimeout(() => {
      channel.exit(code);
    }, 500);
  }
};

// Частные функции
// Строка для создания таблиц в БД
function getCreateTableStr(tableName) {
  let result;
  switch (tableName) {
    case 'timeline':
      result =
        'id int auto_increment NOT NULL,' +
        'dn char(64) NOT NULL,' +
        'start BIGINT NOT NULL,' +
        'end BIGINT NOT NULL,' +
        'state char(8),' +
        'PRIMARY KEY(id)';
      break;

    default:
      result =
        'id INTEGER PRIMARY KEY NOT NULL,ts INTEGER NOT NULL,dn TEXT NOT NULL,prop TEXT,val REAL';
  }
  return 'CREATE TABLE IF NOT EXISTS ' + tableName + ' (' + result + ')';
}
