var child_process = require('child_process');

function getCqlVersion(systemClient) {
    return systemClient.execute("SELECT cql_version FROM system.local")
        .then(result => result.rows[0].cql_version);
}

function runCqlsh(cqlVersion, commandStream, {user, password, host, port}) {
    return new Promise((resolve, reject) => {
        var done = false;
        var stdin = commandStream;
        if (typeof commandStream === 'string' || commandStream instanceof Buffer)
            stdin = "pipe";

        let ps = child_process.spawn(
            "cqlsh", ["--cqlversion", cqlVersion, "-u", user, "-p", password, host, port],
            {stdio: [stdin, "pipe", "inherit"]});
        ps.on('error', reject);
        ps.on('exit', code => {
            if (code == 0 && done)
                resolve(output);
            else if (code == 0) {
                done = true;
            } else {
                reject(new Error(`CQLSH failed with code: ${code}`));
            }
        });

        var output = '';
        ps.stdout.setEncoding('utf-8');
        ps.stdout.on('data', chunk => output += chunk);
        ps.stdout.on('end', () => {
            if (done)
                resolve(output);
            else
                done = true;
        });

        if (typeof commandStream === 'string' || commandStream instanceof Buffer) {
            ps.stdin.setEncoding('utf-8');
            ps.stdin.write(commandStream);
            ps.stdin.end();
        }
    });
}

module.exports = {
    getCqlVersion: getCqlVersion,
    runCqlsh: runCqlsh
}
