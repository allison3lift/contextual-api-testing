require('dotenv').config();
const csv = require('fast-csv');
const fs = require('fs');
const fetch = require("node-fetch");
let btoa = require('btoa');

async function callPeer39Url(url) {
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

async function gteIASAuthToken() {
    const username = 736418;
    const password = process.env.IAS_PASSWORD;

    return await fetch('https://auth.adsafeprotected.com/uaa/oauth/token', {
        method: 'POST',
        body: 'grant_type=client_credentials',
        headers: {
            'Authorization': 'Basic ' + btoa(`${username}:${password}`),
            'Content-Type': 'application/x-www-form-urlencoded'
        },
    })
        .then(res => res.json())
        .then(res => res.access_token)
        .catch(err => console.log(err));
}

async function callIASUrl(url) {
    let authToken = await gteIASAuthToken();
    return new Promise((res, rej) => {
        fetch(url, {
            headers: {
                'Authorization': `bearer ${authToken}`
            }
        }).then(result => result.json())
            .then(result => res(result))
            .catch(err => rej(err));
    });
}

function recordResponses(results) {
    const date = new Date();
    csv.writeToPath(`responses-${date.toISOString().split(':').join('')}.csv`, results, {
        headers: true,
        transform: (row) => {
            return {
                url: row.url,
                peer39Mapped: row.peer39Mapped,
                peer39: row.peer39,
                // gumgum: row.gumgum,
                IASMapped: row.IASMapped,
                IAS: row.IAS
            };
        }
    });
}

function mapPeer39Data(response, categoryData, url) {
    const responseArray = response.split('#');
    const categoryIdsString = responseArray.slice(2, responseArray.length);

    if (categoryIdsString[0] === undefined)
        return;

    const categoryIdsArray = categoryIdsString[0].split(';');

    const mappedCategoriesArray = categoryIdsArray.map(categoryId => {
        const id = categoryId.split(':');
        const categoryObj = categoryData.filter(c => id[0] === c['Category ID'])[0];
        const category = categoryObj === undefined ? id[0] : categoryObj['Peer39 Category'];
        const mappedCategoryWithScore = category + ':' + id[1];
        return mappedCategoryWithScore;
    })
    const categoryMappedIdsString = mappedCategoriesArray.toString().split(',').join(';');
    let responseMapped = `${responseArray.slice(0, 2).toString().split(',').join('#')}#${categoryMappedIdsString}#${categoryIdsString[1]}#${categoryIdsString[2]}`;
    return responseMapped;
}

function mapIASData(response, categoryData) {
    const mappedResponse = response.map(id => {
        const categoryObj = categoryData.filter(c => id === c['id'])[0];
        const category = categoryObj === undefined ? id : categoryObj['category'];
        return category;
    });
    return mappedResponse;
}

async function makeCallsByUrl(urlsObj) {
    const peer39Promise = callPeer39Url(urlsObj['peer39Url']);
    const categoryData = await processPeer39CategoriesCsv();
    // const gumGumPromise = callGumGumUrl(urlsObj['gumGumUrl']);
    const IASPromise = callIASUrl(urlsObj['IASUrl']);
    const IAScategory = await processIASCategoriesCsv();
    return await Promise.all([peer39Promise, IASPromise])
        .then(resp => ({
            url: urlsObj['url'], peer39Mapped: mapPeer39Data(resp[0], categoryData), peer39: resp[0],
            // gumgum: resp[1]
            IASMapped: mapIASData(resp[1]['segment_ids'], IAScategory), IAS: resp[1]['segment_ids']
        }));
}

async function processCalls(urls) {
    const promises = urls
        .map(u => makeCallsByUrl(u));

    console.log("Processing... ");
    console.log("This may take time.");
    await Promise.all(promises)
        .then(response => recordResponses(response))
        .catch(err => console.log(err));

    return;
}

async function processUrlsCsv(fileName) {
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
            .on("end", async () => {
                const results = await processCalls(urls);
                res(results);
            });
    });
}

async function processPeer39CategoriesCsv() {
    return new Promise((res, rej) => {
        let categoryData = [];
        const stream = fs.createReadStream('Sandbox_API_Taxonomy_Partner_Version_2020.csv');
        stream.on('error', () => rej('File name does not exist.'))
        csv
            .parseStream(stream, { headers: true })
            .on("data", (data) => {
                categoryData.push(data);
            })
            .on("end", async () => {
                res(categoryData);
            });
    });
}

async function processIASCategoriesCsv() {
    return new Promise((res, rej) => {
        let categoryData = [];
        const stream = fs.createReadStream('IAS_KEY.csv');
        stream.on('error', () => rej('File name does not exist.'))
        csv
            .parseStream(stream, { headers: true })
            .on("data", (data) => {
                categoryData.push(data);
            })
            .on("end", async () => {
                res(categoryData);
            });
    });
}

function concatUrl(url) {
    const encodedUrl = encodeURI(url[0]);
    const gumGumUrl = `https://verity-api.gumgum.com/page/classify?pageUrl=${encodedUrl}`
    const peer39Url = `http://sandbox.api.peer39.net/proxy/targeting?cc=NwH7OeBv/4cSJEcpby8fbowEVshWUO5xu1soA12uA11=&pu=${encodedUrl}&ct=triplelift`;
    const IASUrl = `https://api.adsafeprotected.com/db2/client/736418/segt?adsafe_url=${encodedUrl}`
    return { url: url[0], gumGumUrl, peer39Url, IASUrl };
}

async function executeApis() {
    const arg = process.argv.splice(1, 1)
    const csvFileName = arg[0].toString();
    let results = await processUrlsCsv(csvFileName);
    console.log("COMPLETE! Results can be found new CSV file.")
    return results;
}

module.exports = {
    executeApis: executeApis
};