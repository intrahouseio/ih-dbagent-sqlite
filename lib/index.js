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

  const options = getOptions(opt);
  let overflow = 0;
  let lastOverflow = 0;
  let maxTimeRead = 0;
  let maxTimeWrite = 0;
  let stmtRecords = {};

  let hoursRule = new schedule.RecurrenceRule();
  // hoursRule.rule = '*/15 * * * * *';
  hoursRule.rule = '0 0 * * * *';

  let j = schedule.scheduleJob(hoursRule, () => {
    send({ id: 'settings', type: 'settings' }); // Get settings for retention policy
  });

  logger.log('Options: ' + JSON.stringify(options), 2);


  async function getDBSize() {
    try {
      let stats = await fs.stat(opt.dbPath);
      let fileSize = stats.size / 1048576;
      stats = await fs.stat(opt.dbPath + '-shm');
      fileSize += stats.size / 1048576;
      stats = await fs.stat(opt.dbPath + '-wal');
      fileSize += stats.size / 1048576;
      if (process.connected) process.send({ type: 'procinfo', data: { size: Math.round(fileSize * 100) / 100 } });
      overflow = fileSize > opt.dbLimit ? 1 : 0;

      if (process.connected) process.send({ type: 'procinfo', data: { overflow } });
    } catch (e) {
      logger.log('getDBSize ERROR: ' + util.inspect(e));
    }

    maxTimeRead = 0;
    maxTimeWrite = 0;
  }

  try {
    await client.createPoolToDatabase(options, logger);
    if (!client.pool) throw { message: 'Client creation Failed!' };

    await client.run('PRAGMA journal_mode = WAL;');
    await client.run('PRAGMA synchronous = NORMAL;');
    await client.run('PRAGMA auto_vacuum = FULL;');

    await client.createTable(getCreateTableStr('records'), 'records');
    await client.run('DROP INDEX IF EXISTS idx_records_ts;');
    await client.run('DROP INDEX IF EXISTS idx_records_dn;');
    await client.run('CREATE INDEX IF NOT EXISTS idx_records_dnts ON records (dn, ts);');

    await client.createTable(getCreateTableStr('timeline'), 'timeline');
    await client.run('CREATE INDEX IF NOT EXISTS idx_records_start ON timeline (start);');

    await client.createTable(getCreateTableStr('customtable'), 'customtable');
    await client.run('CREATE INDEX IF NOT EXISTS idx_name ON customtable (name);');

    // NEW
    await client.createTable(getCreateTableStr('formulas'), 'formulas');
    stmtRecords = client.pool.prepare('INSERT INTO records (ts, dn, prop, val) VALUES (?,?,?,?)');

    // Add strrecords
    await client.createTable(getCreateTableStr('strrecords'), 'strrecords');
    await client.run('CREATE INDEX IF NOT EXISTS idx_strrecords_dnts ON strrecords (dn, ts);');

    getDBSize();
    setInterval(getDBSize, 60000); // Get db size

    channel.on('message', ({ id, type, query, payload, table }) => {
      if (type == 'write' && table == 'customtable') return writeCustom(id, payload, table);
      if (type == 'update') return table == 'customtable' ? updateCustom(query, payload, table) : update(query, payload, table); // [{id, $set:{field1:’newval’}}]
      if (type == 'remove') return removeCustom(payload, table); // []

      if (type == 'write') {
        if (overflow == 0) return write(id, payload, table);
        if (overflow == 1 && lastOverflow == 0) {
          lastOverflow = overflow;
          return sendError(id, 'The allocated space for the database has run out, increase the limit');
        }
      }
      if (type == 'read') return read(id, query);
      if (type == 'settings') return del(payload);
      // NEW
      if (type == 'run') return run(id, query);
      // exit agent
      if (type == 'stop') return processExit(0);
    });

    process.on('SIGTERM', () => {
      logger.log('Received SIGTERM');
      processExit(0);
    });
  } catch (err) {
    processExit(1, err);
  }

  /**
   *
   * @param {String} id - request uuid
   * @param {Array of Objects} payload - [{ dn, prop, ts, val }]
   */
  async function write(id, payload, table) {
    const beginTime = Date.now();

    const tableName = table || 'records';
    try {
      if (tableName == 'records') {
        if (!payload || !payload.length) return;
        client.pool.serialize(() => {
          client.pool.run('BEGIN');
          for (let i = 0; i < payload.length; i++) {
            let value = payload[i];
            stmtRecords.run(value.ts, value.dn, value.prop, value.val);
          }
          client.pool.run('COMMIT');
        });
      } else {
        const columns = getColumns(tableName);
        const values = utils.formValues(payload, columns);
        if (!values || !values.length) return;

        const query = 'INSERT INTO ' + tableName + ' (' + columns.join(',') + ') VALUES ';
        const values1 = values.map(i => `(${i})`).join(', ');
        let sql = query + ' ' + values1;
        await client.run(sql);
      }

      const endTime = Date.now();
      if (maxTimeWrite < endTime - beginTime) {
        maxTimeWrite = endTime - beginTime;
        if (process.connected)
          process.send({
            type: 'procinfo',
            data: { lastMaxTimeWrite: maxTimeWrite, lastMaxCountWrite: payload.length }
          });
      }
      logger.log('Write query id=' + id + util.inspect(payload), 2);
    } catch (err) {
      sendError(id, err);
    }
  }

  async function writeCustom(id, payload, table) {
    logger.log('writeCustom' + util.inspect(payload), 1);
    const columns = getColumns(table);
    const query = 'INSERT INTO ' + table + ' (' + columns.join(',') + ') VALUES ';
    const values = payload.map(i => `('${i.name}', ${i.ts}, json('${i.payload}'))`).join(', ');
    const sql = query + ' ' + values;
    try {
      await client.run(sql);
    } catch (err) {
      sendError(id, err);
    }
  }

  async function updateCustom(query, payload, table) {
    logger.log('updateCustom ' + util.inspect(payload), 1); // [{id, $set:{field1:37, field2: 45}}]
    if (query.sql) {
      try {
        await client.run(query.sql);
      } catch (err) {
        sendError('update', err);
      }
    } else {
      for (j = 0; j < payload.length; j++) {
        const arr = Object.keys(payload[j].$set);
        const values = arr
          .map(i => `'$.${i}', ${typeof payload[j].$set[i] === 'string' ? `'${payload[j].$set[i]}'` : payload[j].$set[i]}`)
          .join(', ');
        const sql = `UPDATE ${table} SET payload = json_set(payload, ${values}) WHERE ID = ${payload[j].id}`;
        logger.log('updateCustom' + sql);
        try {
          await client.run(sql);
        } catch (err) {
          sendError('update', err);
        }
      }
    }
  }

  async function update(query, payload, table) {
    logger.log('update ' + util.inspect(payload), 1); // [{id, $set:{jhead:{field1:37, field2: 45}, desc:'xxx'}}]
    if (query.sql) {
      try {
        await client.run(query.sql);
      } catch (err) {
        sendError('update', err);
      }
    } else {
      for (j = 0; j < payload.length; j++) {
        const fieldsArr = Object.keys(payload[j].$set);
        const updateItems = [];
        for (const field of fieldsArr) {
          const fieldVal = payload[j].$set[field];
          if (typeof fieldVal == 'object') {
            updateItems.push(getJsonSet(field, fieldVal));
          } else {
            updateItems.push(getFieldSet(field, fieldVal));
          }
        }
        if (!updateItems.length) continue;
        const sql = `UPDATE ${table} SET ${updateItems.join(', ')}  WHERE ID = ${payload[j].id}`;
        logger.log(sql, 1);
        try {
          await client.run(sql);
        } catch (err) {
          sendError('update', err);
        }
      }
    }
  }

  function getFieldSet(field, val) {
    return `${field} = '${val}'`; // TODO Для чисел - без кавычек!
  }

  function getJsonSet(field, fobj) {
    const arr = Object.keys(fobj);
    const values = arr.map(i => `'$.${i}', ${getJsonItemValue(fobj[i], field)}`).join(', ');
    return `${field} = json_set(${field}, ${values})`;
  }

  function getJsonItemValue(pItem, field) {
    if (field == 'jrows') return `${pItem}`; // Всегда числа в массиве
    if (typeof pItem === 'string') return `'${pItem}'`;
    if (Array.isArray(pItem)) return ` json_array (${pItem})`;
    return `${pItem}`; //  Число?
  }

  async function removeCustom(payload, table) {
    logger.log('removeCustom' + util.inspect(payload), 1);
    const values = payload.map(i => `${i.id}`).join(', ');
    const sql = `DELETE FROM ${table} WHERE id IN (${values})`;
    try {
      const changes = await client.run(sql);
      logger.log(`Row(s) removed ${changes}`, 2);
    } catch (err) {
      sendError('remove', err);
    }
  }

  async function del(payload) {
    const { rp, rpstr } = payload;
    await delPointsForTable(rp, 'records');
    await delPointsForTable(rpstr, 'strrecords');
  }

  async function delPointsForTable(arr, tableName) {
    if (!arr || !arr.length) return;

    let archDays = [1, 7, 15, 30, 90, 180, 360, 366, 500, 732, 1098];
    for (const archDay of archDays) {
      const arrDnProp = arr.filter(object => object.days == archDay);
      await deletePoints(tableName, archDay, arrDnProp);
    }
  }

  async function deletePoints(tableName, archDay, arrDnProp) {
    logger.log('Archday=' + archDay + ' ArrayofProps=' + JSON.stringify(arrDnProp), 1);
    let archDepth = archDay * 86400000;
    // let archDepth = 600000;
    let delTime = Date.now() - archDepth;
    if (!arrDnProp.length) return;
    while (arrDnProp.length > 0) {
      let chunk = arrDnProp.splice(0, 500);
      let values = chunk.map(i => `(dn='${i.dn}' AND prop='${i.prop}')`).join(' OR ');
      logger.log('Map=' + values, 1);
      let sql = `DELETE FROM ${tableName} WHERE (${values}) AND ts<${delTime}`;
      logger.log(`SQL: ${sql}`, 1);
      try {
        const changes = await client.run(sql);
        logger.log(`Row(s) deleted ${changes}`);
      } catch (err) {
        sendError('delete', err);
      }
    }
  }

  async function read(id, queryObj) {
    const beginTime = Date.now();

    let dnarr;
    logger.log('Read query id=' + id + util.inspect(queryObj), 1);
    try {
      let queryStr;
      if (queryObj.sql) {
        queryStr = queryObj.sql;
      } else {
        if (!queryObj.dn_prop) throw { message: 'Expected dn_prop in query ' };
        dnarr = queryObj.dn_prop.split(',');
        queryStr = utils.getQueryStr(queryObj, dnarr);
      }
      logger.log('SQL: ' + queryStr, 1);

      const result = await client.query(queryStr);
      const endTime = Date.now();
      if (maxTimeRead < endTime - beginTime) {
        maxTimeRead = endTime - beginTime;
        if (process.connected)
          process.send({ type: 'procinfo', data: { lastMaxTimeRead: maxTimeRead, lastMaxCountRead: result.length } });
      }
      logger.log('Get result ' + id, 2);
      let payload = [];
      if (queryObj.data_type == 'calculation') {
        payload = await calculate(queryObj, dnarr, result);
      } else {
        payload = queryObj.target == 'trend' ? formForTrend(result) : result;
      }
      send({ id, query: queryObj, payload });
    } catch (err) {
      sendError(id, err);
    }

    function formForTrend(res) {
      return dnarr.length == 1 ? res.map(item => [item.ts, item.val]) : utils.recordsForTrend(res, dnarr);
    }
  }

  function settings(id, query, payload) {
    logger.log('Recieve settings' + JSON.stringify(payload), 1);
    // if (query.loglevel) logger.setLoglevel(query.loglevel);
  }

  // NEW
  async function run(id, queryObj) {
    try {
      if (!queryObj.sql) throw { message: 'Expect sql clause!' };
      const sql = queryObj.sql;
      logger.log('run:' + util.inspect(sql), 1);
      await client.run(sql);
    } catch (err) {
      sendError(id, err);
    }
  }

  function send(message) {
    if (channel.connected) channel.send(message);
  }

  function sendError(id, err) {
    logger.log(err);
    send({ id, error: utils.getShortErrStr(err) });
  }

  function getOptions(argOpt) {
    const res = {};
    return Object.assign(res, argOpt);
  }

  function processExit(code, err) {
    let msg = '';
    if (err) msg = 'ERROR: ' + utils.getShortErrStr(err) + ' ';
    stmtRecords.finalize();
    if (client && client.pool) {
      client.pool.close(err => {
        if (err) {
          msg += util.inspect(err, null, 4);
        } else {
          msg += 'Close connection pool.';
        }
        client.pool = null;
        logger.log(msg + ' Exit with code: ' + code);

        setTimeout(() => {
          channel.exit(code);
        }, 500);
      });
    }
  }
};

// Частные функции
// Строка для создания таблиц в БД
function getCreateTableStr(tableName) {
  let result;
  switch (tableName) {
    case 'timeline':
      result =
        'id INTEGER PRIMARY KEY NOT NULL,' +
        'dn TEXT NOT NULL,prop TEXT,' +
        'start INTEGER  NOT NULL,' +
        'end INTEGER NOT NULL,' +
        'state char(8)';
      break;

    case 'customtable':
      result = 'id INTEGER PRIMARY KEY AUTOINCREMENT, ' + 'name TEXT NOT NULL, ' + 'ts INTEGER NOT NULL, ' + 'payload TEXT';
      break;

    case 'formulas':
      result =
        'id INTEGER PRIMARY KEY NOT NULL, ' +
        'rid TEXT NOT NULL, ' +
        'title TEXT NOT NULL, ' +
        'description TEXT, ' +
        'comments TEXT, ' +
        'active INTEGER, ' +
        'ts INTEGER, ' +
        'jhead TEXT, ' +
        'jrows TEXT';
      break;

    case 'strrecords':
      result = 'id INTEGER PRIMARY KEY NOT NULL,ts INTEGER NOT NULL,dn TEXT NOT NULL,prop TEXT,val TEXT';
      break;

    case 'records':
      result = 'id INTEGER PRIMARY KEY NOT NULL,ts INTEGER NOT NULL,dn TEXT NOT NULL,prop TEXT,val REAL';
      break;
    default:
      result = 'id INTEGER PRIMARY KEY NOT NULL,ts INTEGER NOT NULL,dn TEXT NOT NULL,prop TEXT,val REAL';
  }
  return 'CREATE TABLE IF NOT EXISTS ' + tableName + ' (' + result + ')';
}

function getColumns(tableName) {
  switch (tableName) {
    case 'timeline':
      return ['dn', 'prop', 'start', 'end', 'state'];
    case 'customtable':
      return ['name', 'ts', 'payload'];
    case 'formulas':
      return ['id', 'rid', 'title', 'active', 'ts', 'description', 'comments', 'jhead', 'jrows'];
    default:
      return ['dn', 'prop', 'ts', 'val'];
  }
}
