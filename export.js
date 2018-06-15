const Promise = require('bluebird');
const cassandra = require('cassandra-driver');
const fs = require('fs');
const jsonStream = require('JSONStream');

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

function getSchemaTypeNames(keyspace) {
    return systemClient.execute('SELECT type_name FROM system_schema.types WHERE keyspace_name = ?',
                                [keyspace])
        .then(result => Array.from(result.rows, row => row.type_name));
}

function getSchemaTypeQueries(keyspace, typeNames) {
    return typeNames.map(typeName => `DESCRIBE TYPE "${keyspace}"."${typeName}"`);
}

function getSchemaTypeDefinitions(cqlVersion, keyspace) {
    return getSchemaTypeNames(keyspace)
        .then(typeNames => {
            if (typeNames.length > 0) {
                var query = getSchemaTypeQueries(keyspace, typeNames).join('; ') + ';';
                return runCqlsh(cqlVersion, query, cqlshOptions);
            } else {
                return null;
            }
        });
}

function getSchemaFunctionNames(keyspace) {
    return systemClient.execute('SELECT function_name FROM system_schema.functions WHERE keyspace_name = ?',
                                [keyspace])
        .then(result => Array.from(result.rows, row => row.function_name));
}

function getSchemaFunctionQueries(keyspace, functionNames) {
    return functionNames.map(functionName => `DESCRIBE FUNCTION "${keyspace}"."${functionName}"`);
}

function getSchemaFunctionDefinitions(cqlVersion, keyspace) {
    return getSchemaFunctionNames(keyspace)
        .then(functionNames => {
            if (functionNames.length > 0) {
                var query = getSchemaFunctionQueries(keyspace, functionNames).join('; ') + ';';
                return runCqlsh(cqlVersion, query, cqlshOptions);
            } else {
                return null;
            }
        });
}

function processSharedSchemaExport(keyspace, path) {
    return getCqlVersion(systemClient)
        .then(cqlVersion => Promise.all([getSchemaTypeDefinitions(cqlVersion, keyspace),
                                         getSchemaFunctionDefinitions(cqlVersion, keyspace)]))
        .then(([typeSchema, functionSchema]) => {
            if (!typeSchema && !functionSchema)
                return null;

            console.log("Exporting keyspace TYPEs and FUNCTIONs to shared schema")

            var schemaFile = fs.createWriteStream(path);
            new Promise((resolve, reject) => {
                schemaFile.on('error', reject);
                schemaFile.on('finish', () => resolve());
                if (typeSchema) {
                    schemaFile.write(typeSchema.replace(new RegExp('CREATE TYPE', 'g'),
                                                        'CREATE TYPE IF NOT EXISTS'));
                    schemaFile.write('\n');
                }
                if (functionSchema) {
                    schemaFile.write(functionSchema.replace(new RegExp('CREATE FUNCTION', 'g'),
                                                            'CREATE FUNCTION IF NOT EXISTS'));
                    schemaFile.write('\n');
                }
                schemaFile.end();
            });
        });
}

function processTableSchema(table, path) {
    return getCqlVersion(systemClient)
        .then(cqlVersion => runCqlsh(cqlVersion, `DESCRIBE TABLE "${KEYSPACE}"."${table}"`))
        .then(schemaData => {
            var schemaFile = fs.createWriteStream(path);
            return new Promise((resolve, reject) => {
                schemaFile.on('error', reject);
                schemaFile.on('finish', () => resolve());
                schemaFile.write(schemaData);
                schemaFile.end();
            });
        });
}

function processTableExport(table) {
    console.log('==================================================');
    console.log('Reading table: ' + table);

    return processTableSchema(table, DIRECTORY + "/" + table + ".cql").then(() => new Promise((resolve, reject) => {
        var jsonfile = fs.createWriteStream(DIRECTORY +"/" + table + '.json');
        jsonfile.on('error', function (err) {
            reject(err);
        });

        var processed = 0;
        var startTime = Date.now();
        jsonfile.on('finish', function () {
            var timeTaken = (Date.now() - startTime) / 1000;
            var throughput = timeTaken ? processed / timeTaken : 0.00;
            console.log('Done with table, throughput: ' + throughput.toFixed(1) + ' rows/s');
            resolve();
        });
        var writeStream = jsonStream.stringify('[', ',', ']');
        writeStream.pipe(jsonfile);

        var query = 'SELECT * FROM "' + table + '"';
        var options = { prepare : true , fetchSize : 1000 };

        client.eachRow(query, [], options, function (n, row) {
            var rowObject = {};
            row.forEach(function (value, key) {
                if (typeof value === 'number') {
                    if (Number.isNaN(value)) {
                        rowObject[key] = {
                            type: "NOT_A_NUMBER"
                        }
                    } else if (Number.isFinite(value)) {
                        rowObject[key] = value;
                    } else if (value > 0) {
                        rowObject[key] = {
                            type: "POSITIVE_INFINITY"
                        }
                    } else {
                        rowObject[key] = {
                            type: "NEGATIVE_INFINITY"
                        }
                    }
                } else {
                    rowObject[key] = value;
                }
            });

            processed++;
            writeStream.write(rowObject);
        }, function (err, result) {

            if (err) {
                reject(err);
                return;
            }

            console.log('Streaming ' + processed + ' rows to: ' + table + '.json');

            if (result.nextPage) {
                result.nextPage();
                return;
            }

            console.log('Finalizing writes into: ' + table + '.json');
            writeStream.end();
        });
    }));
}

function getTables() {
    if (process.env.TABLE)
        return Promise.resolve(process.env.TABLE.split(','));

    var systemQuery = "SELECT columnfamily_name AS table_name FROM system.schema_columnfamilies WHERE keyspace_name = ?";
    if (systemClient.metadata.keyspaces.system_schema) {
        systemQuery = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?";
    }

    console.log('Finding tables in keyspace: ' + KEYSPACE);
    return systemClient.execute(systemQuery, [KEYSPACE]).
        then(result => Array.from(result.rows, row => row.table_name));
}

systemClient.connect()
    .then(() => processSharedSchemaExport(KEYSPACE, DIRECTORY + "/_shared.cql"))
    .then(() => getTables())
    .then(tables => Promise.each(tables, processTableExport))
    .then(() => {
        console.log('==================================================');
        console.log('Completed exporting all tables from keyspace: ' + KEYSPACE);
        var gracefulShutdown = [];
        gracefulShutdown.push(systemClient.shutdown());
        gracefulShutdown.push(client.shutdown());

        return Promise.all(gracefulShutdown)
            .then(function (){
                process.exit();
            })
            .catch(function (err){
                console.log(err);
                process.exit(1);
            });
    })
    .catch(function (err){
        console.log(err);
    });
