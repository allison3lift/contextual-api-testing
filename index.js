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
        fetch(url)
            .then(res => res.text())
            .then(data => res({ url: url.toString(), response: data }))
            .catch((err) => rej({ [url]: err }));
    });
}

function recordResponses(results) {
    const date = new Date();
    csv.writeToPath(`responses.csv-${date.toISOString().split(':').join('')}`, results, {
        headers: true,
        transform: function (row) {
            return {
                Url: row.url,
                Response: row.response,
            };
        }
    });
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

    await Promise.all(promises)
        .then(v => {
            recordResponses(v);
        });

    return { responses, failedUrls };
}

async function processCsv(fileName) {
    return new Promise((res, rej) => {
        let urls = [];
        const stream = fs.createReadStream(fileName);
        stream.on('error', () => rej('File name does not exist. Double check the inputed value and file name are the same.'))
        csv
            .parseStream(stream, { headers: false })
            .on("data", function (data) {
                urls.push(...data);
            })
            .on("end", async function () {
                const results = await makeCalls(urls);
                res(results);
            });
    });
}

async function executePixelValidation() {
    const arg = process.argv.splice(1,1)
    const csvFileName = arg.toString();
    let results = await processCsv(csvFileName);
    console.log("COMPLETE! Results can be found new CSV file.")
    return results;
}

module.exports = {
    executePixelValidation: executePixelValidation
};