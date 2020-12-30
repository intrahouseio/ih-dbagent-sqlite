const sqlite3 = require('sqlite3').verbose();

// open database in memory
let db = new sqlite3.Database('./hist.db', (err) => {
  if (err) {
    return console.error(err.message);
  }
  console.log('Connected to the in-memory SQlite database.');
});
//Create table
//db.run('CREATE TABLE langs(name text)');

let languages = ['C++', 'Python', 'Java', 'C#', 'Go'];
let placeholders = languages.map((language) => '(?)').join(',');
let sql = 'INSERT INTO langs(name) VALUES ' + placeholders;

// output the INSERT statement
console.log(sql);
console.log(placeholders);


db.serialize(() => {

  //db.run(sql, languages)

  db.all(`SELECT * FROM records`, [], (err, rows) => {
    if (err) {
      throw err;
    }
    console.log(rows);
    rows.forEach((row) => {
      console.log(row.name);
    });
  });
});
// close the database connection
db.close((err) => {
  if (err) {
    return console.error(err.message);
  }
  console.log('Close the database connection.');
});
