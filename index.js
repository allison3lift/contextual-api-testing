require('dotenv').config();
const csv = require('fast-csv');
const fs = require('fs');
const fetch = require("node-fetch");

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

function mapPeer39Data(response, categoryData) {
    const responseArray = response.split('#');
    const categoryIdsString = responseArray.slice(2, responseArray.length);
    
    if (categoryIdsString[0] === undefined)
        return;

    const categoryIdsArray = categoryIdsString[0].split(';');

    const mappedCategoriesArray = categoryIdsArray.map(categoryId => {
        const id = categoryId.split(':');
        const categoryObj = categoryData.filter(c => id[0] === c['Category ID'])[0];
        const category = categoryObj === undefined ? id[0] : categoryObj['Peer39 Category'];
        const mappedCategoryWithScore = category + ':'+ id[1];
        return mappedCategoryWithScore;
    })
    const categoryMappedIdsString = mappedCategoriesArray.toString().split(',').join(';');
    let responseMapped = `${responseArray.slice(0, 2).toString().split(',').join('#')}#${categoryMappedIdsString}#${categoryIdsString[1]}#${categoryIdsString[2]}`;
    return responseMapped;
}

async function makeCallsByUrl(urlsObj) {
    const peer39Promise = callPeer39Url(urlsObj['peer39Url']);
    const categoryData = await processPeer39CategoriesCsv();
    const gumGumPromise = callGumGumUrl(urlsObj['gumGumUrl'])
    return await Promise.all([peer39Promise, gumGumPromise])
        .then(resp => ({url: urlsObj['url'], peer39: mapPeer39Data(resp[0], categoryData), gumgum: resp[1]}));
}

async function processCalls(urls) {
    const promises = urls
        .map(u => makeCallsByUrl(u));

    console.log("Processing... ");
    console.log("This may take time.");
console.log('here')
    await Promise.all(promises)
        .then(response => recordResponses(response))

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

function concatUrl(url) {
    const encodedUrl = encodeURI(url[0]);
    const gumGumUrl = `https://verity-api.gumgum.com/page/classify?pageUrl=${encodedUrl}`
    const peer39Url = `http://sandbox.api.peer39.net/proxy/targeting?cc=NwH7OeBv/4cSJEcpby8fbowEVshWUO5xu1soA12uA11=&pu=${encodedUrl}&ct=triplelift`;
    return {url: url[0], gumGumUrl, peer39Url};
}

async function executeApis() {
    const arg = process.argv.splice(1,1)
    const csvFileName = arg[0].toString();
    let results = await processUrlsCsv(csvFileName);
    console.log("COMPLETE! Results can be found new CSV file.")
    return results;
}

module.exports = {
    executeApis: executeApis
};