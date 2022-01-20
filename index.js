'use strict';
const fs = require('fs');
const axios = require('axios')
const Knex = require('knex');
const dotenv = require('dotenv').config();
const Client = require('ssh2-sftp-client');
const csv = require("csvtojson");

// sftp
let dataSet

const sftp_config = {
    host: process.env.SFTP_HOST,
    port: 22,
    username: process.env.SFTP_USER,
    password: process.env.SFTP_PASS
}

const sftp = new Client();

const getUrls = () => {
    const destination = fs.createWriteStream('./test.csv')
    sftp.connect(sftp_config).then(() => {
        return sftp.get('/Interview Task - Categorisation.csv', destination)
    }).then(() => {
        sftp.end()
        console.log("got the file!")
        urlsToJson()
    }).catch(error => {
        console.log(error.message);
    });
}

// parse csv file to JSON

const urlsToJson = () => {
    csv()
    .fromFile('./test.csv')
    .then(data => {
        dataSet = data
        getKeywords(data)
    })
}

// get keywords for each url (using "organic search")

const fetchParams = {
    type: 'domain_organic',
    key: process.env.SEMRUSH,
    display_filter:' %2B%7CPh%7CCo%7Cseo',
    display_limit: 10,
    export_columns: 'Ph,Po,Pp,Pd,Nq,Cp,Ur,Tr,Tc,Co,Nr,Td',
    display_sort: 'tr_desc',
    database: 'us'
}

const getKeywords = async (arr) => {
    const promiseArr = []

    await arr.forEach(item => {
        const params = {
            ...fetchParams,
            domain: item.URL
        }
        promiseArr.push(axios.get('https://api.semrush.com/', {params: params}))
    })

    const result = await Promise.all(promiseArr)

    let keywordsPromiseArr = []

    result.map(({ data }) => {
        keywordsPromiseArr.push(
            csv({delimiter: ";"})
            .fromString(String(data))
        )
    })

    let keywordPromises = await Promise.all(keywordsPromiseArr)
    // FINAL KEYWORD ARRAY
    let jsonArray = keywordPromises.map(data => data)

    // combine arrays (to make my life easier pushing to db)
    const finalArray = []

    for (let x = 0; x < arr.length; x++) {
        jsonArray[x].forEach(element => {
            element['original_url'] = arr[x].URL
            element['category'] = arr[x].Category
        })
        finalArray.push(...jsonArray[x])
    }

    pushToDb(finalArray)
}


// Push to Database

// db details
const pg = require('knex')(({
    client: 'pg',
    // connection: process.env.DATABASE_URL
    connection: {
        port : 5432,
        user : process.env.PG_USER,
        password : process.env.PG_PASS,
        database : process.env.PG_DB
      }
}))

const pushToDb = (finArr) => {
    // map to knex format
    const arrToPush = finArr.map(item => ({
        main_url: item.original_url,
        result_url: item.Url,
        category: item.category,
        keyword: item.Keyword,
        keyword_position: item.Position
    }))

    pg('kaizen')
    .insert(arrToPush)
    .then(() => console.log("pushed to database!"))
    .catch(error => console.log(error.message))
}

getUrls()

