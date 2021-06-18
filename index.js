const csv = require('fast-csv');
const fs = require('fs');
const fetch = require("node-fetch");

function validUrl(str) {
    try {
        return true;
    }
    catch {
        return false;
    }
}

function callUrl(url) {
    return new Promise((res, rej) => {
        fetch(url, {headers: {
            'Accept-Encoding': 'gzigzip, deflate, brp',
          }})
          .then(res => res.text())
          .then(data => res({url: url.toString(), response: data}))
        .catch((err) => rej({ [url]: err }));
    });
}

function recordResponses(results, ws) {
    console.log(results);
    csv.write(results, {headers:true} )
    .pipe(ws);
}

async function makeCalls(urls) {
    const invalidUrls = urls
        .filter(u => !validUrl(u));

    // TODO write to the db url: failed 
    let failedUrls = [...invalidUrls];


    const promises = urls
        .filter(u => validUrl(u))
        .map(u => callUrl(new URL(u)));

    console.log("Processing... ");
    console.log("This may take time for large datasets.");

    var responses = [];

    let ws = fs.createWriteStream('results.csv');

    await Promise.all(promises)
        .then(v => {
            recordResponses(v, ws);
        });

    ws.end();
    return { responses, failedUrls };
}

async function processCsv() {
    return new Promise(res => {
        let urls = [];

        var stream = fs.createReadStream("./urls.csv");
        
        csv
         .parseStream(stream, {headers : false})
         .on("data", function(data){
            urls.push(...data);
         })
         .on("end", async function(){
            const results = await makeCalls(urls);
            // console.log(results);
            res(results);
         });
    });
}

async function executePixelValidation() {
    let results = await processCsv();
    console.log("COMPLETE! Results can be found in database.")
    // console.log(results)
    return results;
}

module.exports = {
    executePixelValidation: executePixelValidation
};