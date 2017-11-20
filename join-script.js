#!/usr/bin/env node

const _ = require('lodash');
const fs = require('fs');
const csv = require('csvtojson');
const converter = require('json-2-csv');

function printUsage(exitCode) {
  console.log('');
  console.log('    Joins data in source CSV files on Address_Number and Street fields and writes output as CSV.');
  console.log('');
  console.log('        Usage: join-script <input-file-1.csv> <input-file-1.csv> <output-file.csv>');
  process.exit(exitCode);
}

if (process.argv.length !== 5) {
  console.error('Invalid command line.');
  printUsage(1);
}

const inputFileNames = [process.argv[2], process.argv[3]];
const outputFileName = process.argv[4];

if (fs.existsSync(outputFileName)) {
  console.error('Error: output file exists.');
  printUsage(1);
}

console.log('Joining data in', inputFileNames);

const inputFileContents = [];

return readInputFileContents()
  .then(function() {
    let noMatchCount = 0;
    _.each(inputFileContents[0], function(jsonObj) {
      const match = _.find(inputFileContents[1], { Address_Number: jsonObj.Address_Number, Street: jsonObj.Street });
      if (!match) {
        noMatchCount++;
      } else {
        _.extend(jsonObj, match);
      }
    });

    console.log(`No matches for ${noMatchCount} records.`);

    converter.json2csv(inputFileContents[0], function(error, csv) {
      if (error) {
        console.log('Error converting output to CSV:', error);
        printUsage(1);
      }

      console.log('Writing output to', outputFileName);
      fs.writeFileSync(outputFileName, csv);
    }, {
      checkSchemaDifferences: false
    });
  })
  .catch(function(error) {
    console.log('Error:', error);
    printUsage(1);
  })


function readInputFileContents() {
  const promises = [];
  _.each(inputFileNames, function(fileName) {
    promises.push(readCsvFile(fileName)
      .then(function(fileJson) {
        inputFileContents.push(fileJson);
      }));
  });
  return Promise.all(promises);
}

function readCsvFile(fileName) {
  return new Promise(function(resolve, reject) {
    const fileJson = [];

    const parsed = csv().fromFile(fileName)
      .on('json', (jsonObj) => {
        if (jsonObj.Street && !_.endsWith(jsonObj.Street, ' Street')) {
          jsonObj.Street += ' Street';
        }

        fileJson.push(jsonObj);
      })
      .on('done', (error) => {
        if (error) {
          console.error('Error parsing CSV file', fileName, error);
          return reject(error);
        } else {
          return resolve(fileJson);
        }
      });
  })
}