/**
 *  utils.js
 */
// const util = require('util');

/**
 * Формировать массив для записи в БД
 *
 * @param {Array of Objects} - data - [{ts:1423476765, dn:DD1, prop:'value', val:123},..]
 * @param {Array of Strings}  columns - массив имен столбцов ['ts','dn','val']
 * @param {timestamp} tdate - опционально - дата, за которую надо брать данные
 *
 * Возвращает массив массивов в порядке массива столбцов: [[1423476765, 'DD1', 123],[..]]
 **/
function formValues(data, columns, tdate) {
  let result = [];
  let arr;
  let clmn;

  for (let i = 0; i < data.length; i++) {
    // if (!data[i].ts || (tdate && !hut.isTheSameDate(new Date(data[i].ts), tdate))) continue;
    arr = [];
    for (let j = 0; j < columns.length; j++) {
      clmn = columns[j];
      if (typeof data[i][clmn] === 'undefined') {
        data[i][clmn] = null;
      }
      if (typeof data[i][clmn] === 'number') {
        arr.push(data[i][clmn]);
      } else {
        arr.push("'" + data[i][clmn] + "'");
      }
    }
    result.push(arr);
  }
  return result;
}

/**
 * Формировать данные, считанные из БД, для отдачи на график
 *
 * @param {Array of Objects} records - [{dn,prop,ts,val},...]
 * @param {Array} dnarr ['POO1.temp1','POO1.temp2']
 * @return {Array of Arrays} : [[1608574986578, null,42],[1608574986580, 24,null],...]
 */
function recordsForTrend(records, dnarr) {
  if (!dnarr || !dnarr.length || !records || !records.length) return [];

  // const dArr = dnarr.map(item => item.split('.')[0]);
  const dArr = dnarr;
  const rarr = [];
  const len = dArr.length;
  let last_ts;

  for (let i = 0; i < records.length; i++) {
    const rec = records[i];

    if (!rec || !rec.dn || !rec.prop || !rec.ts) continue;

    const dn_prop = rec.dn + '.' + rec.prop;
    const dn_indx = dArr.findIndex(el => el == dn_prop);
    // const dn_indx = dArr.findIndex(el => el == rec.dn);
    if (dn_indx < 0) continue;

    let data;
    const ts = rec.ts;
    // multiple series data combine
    if (ts != last_ts) {
      data = new Array(len + 1).fill(null);
      data[0] = ts;
      last_ts = ts;
    } else {
      data = rarr.pop();
    }
    data[1 + dn_indx] = Number(rec.val);
    rarr.push(data);
  }
  return rarr;
}

function getQueryStr(query, dnarr) {
  // Время и значения должны быть в одинарных скобках, имена полей в двойных!!
  const from = query.start;
  const to = query.end;
  let res =
    query.table == 'timeline'
     ? `select * from timeline ${formWhereQuery(dnarr, from, to, 'end', 'start')} order by start`
     //  ? `select * from timeline order by start`
      : `select * from records ${formWhereQuery(dnarr, from, to)} order by ts`;
  return res;
}

function formWhereQuery(dnarr, from, to, ts_start_name = 'ts', ts_end_name = 'ts') {
  let query = '';
  let first = true;

  if (dnarr && dnarr.length > 0) {
    if (dnarr.length == 1) {
      query += dnAndProp(dnarr[0]);
      first = false;
    } else {
      query += ' ( ';
      for (let i = 0; i < dnarr.length; i++) {
        if (dnarr[i]) {
          query += isFirst(' OR ') + ' (' + dnAndProp(dnarr[i]) + ')';
        }
      }
      query += ' ) ';
    }
  }

  if (from) {
    query += isFirst(' AND ') + ' ' + ts_start_name + ' >= ' + from;
  }

  if (to) {
    query += isFirst(' AND ') + ' ' + ts_end_name + ' <= ' + to;
  }

  return query ? ' WHERE ' + query : '';

  function isFirst(op) {
    return first ? ((first = false), '') : op;
  }

  function dnAndProp(dn_prop) {
    if (dn_prop.indexOf('.') > 0) {
      const splited = dn_prop.split('.');
      return " dn = '" + splited[0] + "' AND prop = '" + splited[1] + "' ";
    }
    // Иначе это просто dn
    return " dn = '" + dn_prop + "'";
  }
}

function getShortErrStr(e) {
  if (typeof e == 'object') return e.message ? getErrTail(e.message) : JSON.stringify(e);
  if (typeof e == 'string') return e.indexOf('\n') ? e.split('\n').shift() : e;
  return String(e);

  function getErrTail(str) {
    let idx = str.lastIndexOf('error:');
    return idx > 0 ? str.substr(idx + 6) : str;
  }
}

function getDateStr() {
  const dt = new Date();
  return (
    pad(dt.getDate()) +
    '.' +
    pad(dt.getMonth() + 1) +
    ' ' +
    pad(dt.getHours()) +
    ':' +
    pad(dt.getMinutes()) +
    ':' +
    pad(dt.getSeconds()) +
    '.' +
    pad(dt.getMilliseconds(), 3)
  );
}

function pad(str, len = 2) {
  return String(str).padStart(len, '0');
}

module.exports = {
  formValues,
  recordsForTrend,
  getQueryStr,
  formWhereQuery,
  getShortErrStr,
  getDateStr
};
