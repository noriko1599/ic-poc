const {
    EventStoreDBClient,
} = require("@eventstore/db-client");
const { readFileSync } = require("fs")
const rootCertificate = readFileSync(__dirname + '/ca.crt')

const client = new EventStoreDBClient(
    {
        endpoint: "eventstore.decoder159-dev.ml:2113",
    },
    {
        rootCertificate
    },
    {
        username: "admin",
        password: "realdope!.",
    }
);

module.exports = client;