var fs = require('fs');
var path = require('path');
var child_process = require('child_process');

var Promise = require('bluebird');
var _ = require('lodash');
var cassandra = require('cassandra-driver');
var jsonStream = require('JSONStream');

const {runCqlsh, getCqlVersion} = require('./cqlsh');


var HOST = process.env.HOST || '127.0.0.1';
var PORT = process.env.PORT || 9042;
var KEYSPACE = process.env.KEYSPACE;

if (!KEYSPACE) {
    console.log('`KEYSPACE` must be specified as environment variable');
    process.exit();
}

var USER = process.env.USER;
var PASSWORD = process.env.PASSWORD;
var DIRECTORY = process.env.DIRECTORY || "./data";
var cqlshOptions = {user: USER, password: PASSWORD, host: HOST, port: PORT};

var authProvider;
if (USER && PASSWORD) {
    authProvider = new cassandra.auth.PlainTextAuthProvider(USER, PASSWORD);
}

var systemClient = new cassandra.Client({contactPoints: [HOST], authProvider: authProvider, protocolOptions: {port: [PORT]}});
var client = new cassandra.Client({ contactPoints: [HOST], keyspace: KEYSPACE, authProvider: authProvider, protocolOptions: {port: [PORT]}});

function buildTableQueryForDataRow(tableInfo, row) {
    var queries = [];
    var isCounterTable = _.some(tableInfo.columns, function(column) {return column.type.code === 5;});
    row = _.omitBy(row, function(item) {return item === null});
    var query = 'INSERT INTO "' + tableInfo.name + '" ("' + _.keys(row).join('","') + '") VALUES (?' + _.repeat(',?', _.keys(row).length-1) + ')';
    var params = _.values(row);
    if (isCounterTable) {
        var primaryKeys = [];
        primaryKeys = primaryKeys.concat(_.map(tableInfo.partitionKeys, function(item){return item.name}));
        primaryKeys = primaryKeys.concat(_.map(tableInfo.clusteringKeys, function(item){return item.name}));
        var primaryKeyFields = _.pick(row, primaryKeys);
        var otherKeyFields = _.omit(row, primaryKeys);
        var setQueries = _.map(_.keys(otherKeyFields), function(key){
            return '"'+ key +'"="'+ key +'" + ?';
        });
        var whereQueries = _.map(_.keys(primaryKeyFields), function(key){
            return '"'+ key +'"=?';
        });
        query = 'UPDATE "' + tableInfo.name + '" SET ' + setQueries.join(', ') + ' WHERE ' + whereQueries.join(' AND ');
        params = _.values(otherKeyFields).concat(_.values(primaryKeyFields));
    }
    params = _.map(params, function(param){
        if (_.isPlainObject(param)) {
            if (param.type === 'Buffer') {
                return Buffer.from(param);
            } else if (param.type === 'NOT_A_NUMBER') {
                return Number.NaN;
            } else if (param.type === 'POSITIVE_INFINITY') {
                return Number.POSITIVE_INFINITY;
            } else if (param.type === 'NEGATIVE_INFINITY') {
                return Number.NEGATIVE_INFINITY;
            } else {
                var omittedParams = _.omitBy(param, function(item) {return item === null});
                for (key in omittedParams) {
                    if (_.isObject(omittedParams[key]) && omittedParams[key].type === 'Buffer') {
                        omittedParams[key] = Buffer.from(omittedParams[key]);
                    }
                }
                return omittedParams;
            }
        }
        return param;
    });
    return {
        query: query,
        params: params,
    };
}

function loadSchema(path) {
    return new Promise((resolve, reject) => {
        let stream = fs.createReadStream(path, {encoding: 'utf8'});
        stream.on('open', () => resolve(stream));
        stream.on('error', (err) => {
            if (err.code === 'ENOENT')
                resolve(null);
            else {
                console.log(`Failed to load schema at ${schemaPath}`)
                reject(err);
            }
        });
    });
}

function prepareSharedSchema(path) {
    return Promise.all([loadSchema(path), getCqlVersion(systemClient)])
        .then(([schemaStream, cqlVersion]) => runCqlsh(cqlVersion, schemaStream, cqlshOptions))
        .then(() => systemClient.metadata.refreshKeyspace(KEYSPACE));
}

function sleep (time) {
    return new Promise((resolve) => setTimeout(resolve, time));
}

function prepareTable(table, schemaPath) {
    let getSchemaStream = () => loadSchema(schemaPath);
    let getTableMetadata = () => {
        return systemClient.metadata.refreshKeyspace(KEYSPACE)
            .then(() => systemClient.metadata.getTable(KEYSPACE, table));
    };

    return Promise.all([getSchemaStream(), getTableMetadata(), getCqlVersion(systemClient)])
        .then(([schemaStream, tableMetadata, cqlVersion]) => {
            if (tableMetadata && schemaStream) {
                console.log("Recreating table: " + table);
                return client.execute(`DROP TABLE "${KEYSPACE}"."${table}"`)
                    .then(() => sleep(2000))
                    .then(() => runCqlsh(cqlVersion, schemaStream, cqlshOptions))
                    .then(() => getTableMetadata());
            } else if (!tableMetadata && schemaStream) {
                console.log("Creating table: " + table);
                return runCqlsh(cqlVersion, schemaStream, cqlshOptions)
                    .then(_ => getTableMetadata());
            } else if (!tableMetadata && !schemaStream) {
                throw new Exception("Table is missing, but schema is not available for recreation: " + table);
            } else {
                return Promise.resolve(tableMetadata);
            }
        })
}


function processTableImport(table) {
    return prepareTable(table, DIRECTORY + '/' + table + '.cql').
        then(tableInfo => new Promise((resolve, reject) => {
            console.log('Creating read stream from: ' + table + '.json');
            var jsonfile = fs.createReadStream(DIRECTORY + '/' + table + '.json', {encoding: 'utf8'});
            var readStream = jsonfile.pipe(jsonStream.parse('*'));
            var queryPromises = [];
            var processed = 0;

            readStream.on('data', function(row){
                var query = buildTableQueryForDataRow(tableInfo, row);
                queryPromises.push(client.execute(query.query, query.params, { prepare: true}));
                processed++;

                if (processed % 1000 === 0) {
                    console.log('Streaming ' + processed + ' rows to table: ' + table);
                    jsonfile.pause();
                    Promise.all(queryPromises)
                        .then(function (){
                            queryPromises = [];
                            jsonfile.resume();
                        })
                        .catch(reject);
                }
            });
            jsonfile.on('error', reject);

            var startTime = Date.now();
            jsonfile.on('end', function () {
                console.log('Streaming ' + processed + ' rows to table: ' + table);
                Promise.all(queryPromises)
                    .then(function (){
                        var timeTaken = (Date.now() - startTime) / 1000;
                        var throughput = timeTaken ? processed / timeTaken : 0.00;
                        console.log('Done with table, throughput: ' + throughput.toFixed(1) + ' rows/s');
                        resolve();
                    })
                    .catch(reject);
            });
        }));
}

var envTables = process.env.TABLE ? process.env.TABLE.split(",") : null;

var dumpTables = fs.readdirSync(DIRECTORY)
    .filter(fname => fname.endsWith(".json"))
    .map(fname => path.basename(fname, ".json"))
    .filter(table => !envTables || envTables.indexOf(table) != -1);

console.log(`Processing tables: ${dumpTables}`)

systemClient.connect()
    .then(() => prepareSharedSchema(DIRECTORY + '/_shared.cql'))
    .then(() => Promise.each(dumpTables, processTableImport))
    .then(() => Promise.all([systemClient.shutdown(), client.shutdown()]))
    .then(() => console.log('Completed importing to keyspace: ' + KEYSPACE))
    .catch(function (err){
        console.log(err);
    });
