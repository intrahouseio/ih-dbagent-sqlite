const sqlite3 = require('sqlite3').verbose();
var fs = require("fs"); //Load the filesystem module

var stats = fs.statSync("/var/lib/ih-v5/projects/demo_1608708201529/db/hist.db")
var fileSizeInBytes = stats["size"];
// open database in memory
let db = new sqlite3.Database('/var/lib/ih-v5/projects/demo_1608708201529/db/hist.db', (err) => {
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
//console.log(sql);
//console.log(placeholders);


db.serialize(() => {

  //db.run(sql, languages)
  db.all('SELECT Count (*) From records', [], (err, rows) => {
    if (err) {
      throw err;
    }
    console.log(fileSizeInBytes);
    console.log(rows);
  });
  /*db.all(`SELECT * FROM records`, [], (err, rows) => {
    if (err) {
      throw err;
    }
    console.log(rows);
    rows.forEach((row) => {
      console.log(row.name);
    });
  });*/
});
// close the database connection
db.close((err) => {
  if (err) {
    return console.error(err.message);
  }
  console.log('Close the database connection.');
});
