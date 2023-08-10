/**
 * rollup.js
 * Свертка массива, полученного из БД
 *
 * @param {*} arr
 * @param {*} discrete
 * @param {*} cols
 */

const util = require('util');

module.exports = function rollup(arr, discrete, cols) {
  if (!arr || !Array.isArray(arr) || !cols || !Array.isArray(cols)) return [];

  if (arr.length <= 0) return [];

  // В зависимости от дискреты заполнить поле dtx из ts (YYMMDDHH)

  for (let i = 0; i < arr.length; i++) {
    if (arr[i].ts) arr[i].dtx = discrete ? transform(arr[i].ts, discrete) : arr[i].ts;
  }
 
  let sn = 0;
  let j = 0;
  let curdtx = arr[j].dtx;

  let vals = {};
  let index = {};

  cols.forEach((item, idx) => {
    if (item.dn && item.col_type == 'value') {
      // calcs[idx] = item.calc_type;
      if (!index[item.dn]) index[item.dn] = [];
      index[item.dn].push(idx);
    }
  });
  if (cols[1].calc_type == 'optime1') {
    return calcOptime1();
  }

  return calcStandard();

  /*
  let result = []; // Результат - массив массивов
  let curdtx = arr[j].dtx;
  let dn;
  let curval;
  while (j < arr.length) {
    if (curdtx == arr[j].dtx) {
      dn = arr[j].dn+'.'+arr[j].prop;
      
      curval = Number(arr[j].val);

      // Устройство участвует в отчете
      if (index[dn]) {
        index[dn].forEach(idx => {
          if (vals[idx] == undefined) initVal(idx, curval);
          calcVal(idx, curval);
        });
      }
      j++;
    } else {
      result.push(getOneRow());
      curdtx = arr[j].dtx;
      vals = {};
    }
  }
  result.push(getOneRow());
  return result;
  */

  function calcOptime1() {
    const result = [];

    let dn;
    let curval;
    while (j < arr.length) {
      if (curdtx == arr[j].dtx) {
        dn = arr[j].dn + '.' + arr[j].prop;

        curval = Number(arr[j].val);

        // Устройство участвует в отчете
        if (index[dn]) {
          index[dn].forEach(idx => {
            if (vals[idx] == undefined) vals[idx] = { intervalSec: 0, ts0_1: 0, val: 0 };

            if (vals[idx].val != curval) {
              if (curval == 1) {
                vals[idx].ts0_1 = arr[j].ts;
              } else if (curval == 0 && vals[idx].ts0_1 > 0) {
                // переход в 0

                vals[idx].intervalSec += Math.round((arr[j].ts - vals[idx].ts0_1) / 1000);
                vals[idx].ts0_1 = 0;
              }
              vals[idx].val = curval;
            }
          });
        }
        j++;
      } else {
   
        result.push(getOneRow('intervalSec'));
        // Если скачок - нужно сформировать недостающие дискреты?
        if (Number(arr[j].dtx) - Number(curdtx) > 1) {
          fillGap(arr[j].dtx);
        }
        curdtx = arr[j].dtx;
        // vals = {};
      }
    }
    result.push(getOneRow('intervalSec'));
    return result;
  
  function fillGap(nextDataDtx) {
    while (Number(nextDataDtx) - Number(curdtx) > 1) {
      curdtx = transform(nextTsFromDtx(curdtx, discrete), discrete);
      result.push(getOneRow('intervalSec'));
    }
    
  }
}

  function calcStandard() {
    const result = [];
    let dn;
    let curval;
    while (j < arr.length) {
      if (curdtx == arr[j].dtx) {
        dn = arr[j].dn + '.' + arr[j].prop;

        curval = Number(arr[j].val);

        // Устройство участвует в отчете
        if (index[dn]) {
          index[dn].forEach(idx => {
            if (vals[idx] == undefined) initVal(idx, curval);
            calcVal(idx, curval);
          });
        }
        j++;
      } else {
        result.push(getOneRow());
        curdtx = arr[j].dtx;
        vals = {};
      }
    }
    result.push(getOneRow());
    return result;
  }

  function initVal(idx, val) {
    let ival = val;
    if (cols[idx] && cols[idx].calc_type) {
      switch (cols[idx].calc_type) {
        case 'min':
        case 'max':
          ival = val;
          break;

        case 'sum':
          ival = 0;
          break;
        case 'optime1':
          ival = 0;
          break;

        default:
          ival = val;
      }
    }
    vals[idx] = ival;
  }

  function calcVal(idx, val) {
    if (cols[idx] && cols[idx].calc_type) {
      switch (cols[idx].calc_type) {
        case 'sum':
          vals[idx] += val;
          break;

        case 'min':
          if (val < vals[idx]) vals[idx] = val;
          break;

        case 'max':
          if (val > vals[idx]) vals[idx] = val;
          break;

        case 'optime1':
          break;

        default:
          vals[idx] = val;
      }
    } else {
      vals[idx] = val;
    }
  }

  function getOneRow(valProp) {
    let one = [];
    let val;

    cols.forEach((item, idx) => {
      switch (item.col_type) {
        case 'sn': // Номер по порядку
          sn += 1;
          val = String(sn);
          break;

        case 'value': // Значение
          if (typeof vals[idx] == 'object') {
            if (valProp == 'intervalSec') {
              val = calcInterval(vals[idx]);
            } else {
              val = vals[idx][valProp] || null;
            }
          } else val = vals[idx] || null;
          break;

        case 'date': // Дата-время
          if (discrete) {
            val = getTsFromDtx(curdtx, discrete);
          } else val = curdtx;
          break;

        default:
          val = '';
      }
      one.push(val);
    });
    return one;
  }

  function calcInterval(valsItem) {
   
    // { intervalSec: 0, ts0_1: ts, val: 1 };
    // Сохранить на начало следующего интервала, если val = 1
    if (valsItem.val == 1 && valsItem.ts0_1 > 0) {
      const nextTs = nextTsFromDtx(curdtx, discrete);
      // Учесть до точки перехода
      valsItem.intervalSec += Math.round((nextTs - valsItem.ts0_1 - 1) / 1000);
      valsItem.ts0_1 = nextTs;
    } else {
      valsItem.ts0_1 = 0;
      valsItem.val = 0;
    }

    let result = valsItem.intervalSec;
    valsItem.intervalSec = 0;

    // Рассчитать в минутах, часах или днях в зависимости от дискреты
    return fromSecTo(result, discrete);
  }
};

function fromSecTo(val, discrete) {
  if (discrete == 'hour') return Math.round(val/60); 
  if (discrete == 'day') return Math.round(val/360)/10; 
  if (discrete == 'month') return Math.round(val/(24*360))/10; 

}

// Преобразовать в зависимости от дискреты
function transform(ts, discrete) {
  let dt = new Date(ts);
  let dtx = String(dt.getFullYear() - 2000);
  dtx += pad(dt.getMonth());
  if (discrete == 'month') return dtx;

  dtx += pad(dt.getDate());
  if (discrete == 'day') return dtx;

  dtx += pad(dt.getHours());
  if (discrete == 'hour') return dtx;

  dtx += pad(dt.getMinutes());
  return dtx;
}

function getTsFromDtx(dtx, discrete) {
  let yy = Number(dtx.substr(0, 2)) + 2000;
  let mm = Number(dtx.substr(2, 2));
  let dd = 0;
  let hh = 0;
  let minutes = 0;

  if (discrete == 'month') {
    dd = 1;
    hh = 0;
  } else {
    dd = Number(dtx.substr(4, 2));
    if (discrete == 'day') {
      hh = 0;
    } else {
      hh = Number(dtx.substr(6, 2));
      if (discrete == 'hour') {
        minutes = 0;
      } else {
        minutes = Number(dtx.substr(8, 2));
      }
    }
  }

  return new Date(yy, mm, dd, hh, minutes).getTime();
}

function nextTsFromDtx(dtx, discrete) {
  const dt = new Date(getTsFromDtx(dtx, discrete));
  if (discrete == 'month') {
    dt.setMonth(dt.getMonth() + 1);
  } else if (discrete == 'day') {
    dt.setDate(dt.getDate() + 1);
  }
  if (discrete == 'hour') {
    dt.setHours(dt.getHours() + 1);
  }
  return dt.getTime();
}

function pad(val, width) {
  let numAsString = val + '';
  width = width || 2;
  while (numAsString.length < width) {
    numAsString = '0' + numAsString;
  }
  return numAsString;
}
