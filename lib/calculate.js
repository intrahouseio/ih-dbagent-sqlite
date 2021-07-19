/**
 *
 */

const rollup = require('./rollup');

module.exports = async function(query, dnArr, dataArr) {
  let cols = dnArr.map(dn => ({ col_type: 'value', dn, calc_type: query.calc_type || 'sum' }));
  cols.unshift({ col_type: 'date' });

  const discrete = query.discrete || 'hour';
  return rollup(dataArr, discrete, cols);
};
