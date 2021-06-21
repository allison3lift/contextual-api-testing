require('dotenv').config();
const csv = require('fast-csv');
const fs = require('fs');
const fetch = require("node-fetch");

function callPeer39Url(url) {
    return new Promise((res, rej) => {
        fetch(url)
            .then(res => res.text())
            .then(data => res(data))
            .catch((err) => rej({ [url]: err }));
    });
}

function callGumGumUrl(url) {
    return new Promise((res, rej) => {
        fetch(url, {
            headers: {
                'x-api-key': process.env.GUMGUM_API_KEY
            }
        })
            .then(res => res.text())
            .then(data => res(data))
            .catch((err) => rej({ [url]: err }));
    });
}

function recordResponses(results) {
    const date = new Date();
    csv.writeToPath(`responses-${date.toISOString().split(':').join('')}.csv`, results, {
        headers: true,
        transform: (row) => {
            return {
                url: row.url,
                peer39: row.peer39,
                gumgum: row.gumgum
            };
        }
    });
}

async function makeCallsByUrl(urlsObj) {
    const peer39Promise = callPeer39Url(urlsObj['peer39Url']);
    const gumGumPromise = callGumGumUrl(urlsObj['gumGumUrl'], )
    return await Promise.all([peer39Promise, gumGumPromise])
        .then(resp => ({url: urlsObj['url'], peer39: resp[0], gumgum: resp[1]}));
}

async function makeCalls(urls) {
    const promises = urls
        .map(u => makeCallsByUrl(u));

    console.log("Processing... ");
    console.log("This may take time.");

    await Promise.all(promises)
        .then(response => recordResponses(response))

    return;
}

async function processCsv(fileName) {
    return new Promise((res, rej) => {
        let urls = [];
        const stream = fs.createReadStream(fileName);
        stream.on('error', () => rej('File name does not exist. Double check the inputed value and file name are the same.'))
        csv
            .parseStream(stream, { headers: false })
            .on("data", (data) => {
                const concatUrls = concatUrl(data);
                urls.push(concatUrls);
            })
            .on("end", async function () {
                const results = await makeCalls(urls);
                res(results);
            });
    });
}

function concatUrl(url) {
    const encodedUrl = encodeURI(url[0]);
    const gumGumUrl = `https://verity-api.gumgum.com/page/classify?pageUrl=${encodedUrl}`
    const peer39Url = `http://sandbox.api.peer39.net/proxy/targeting?cc=NwH7OeBv/4cSJEcpby8fbowEVshWUO5xu1soA12uA11=&pu=${encodedUrl}&ct=triplelift`;
    return {url: url[0], gumGumUrl, peer39Url};
}

async function executeApis() {
    const arg = process.argv.splice(1,1)
    const csvFileName = arg[0].toString();
    let results = await processCsv(csvFileName);
    console.log("COMPLETE! Results can be found new CSV file.")
    return results;
}

module.exports = {
    executeApis: executeApis
};