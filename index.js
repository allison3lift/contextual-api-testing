const csv = require('fast-csv');
const fs = require('fs');
const fetch = require("node-fetch");
const convert = require("xml2json");

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
          .then(data => {
            console.log(data)
          })
        .catch((err) => rej({ [url]: err }));
    });
}

function recordResponse() {
    // BUILD OUT
}

async function makeCalls(urls) {
    const invalidUrls = urls
        .filter(u => !validUrl(u));

    // TODO write to the db url: failed 
    let failedUrls = [...invalidUrls];

    var responses = [];

    const promises = urls
        .filter(u => validUrl(u))
        .map(u => callUrl(new URL(u)));

    console.log("Processing... ");
    console.log("This may take time for large datasets.");

    await Promise.all(promises).then(v => {
        console.log(v[0])
        // let output = await zlib.inflateSync(compressed);
        // console.log(output);

        // v.map(response =>
        //     Object.entries(response).forEach(([key, value, index]) => {
        //         console.log(response.b)
        //         // if (value.toString().includes('2') || value.toString().includes('3'))
        //             // {console.log(value);
        responses.push(v[0]);
        //             // recordResponse() <<< BUILD OUT 
        //         // else {
        //             // TODO write to the db url: failed 
        //             // failedUrls.push(key)
        //         // }
        //     }))
    }).catch(err => console.log(err));

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