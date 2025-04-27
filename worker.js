//@ts-check
const { parentPort } = require('worker_threads');
const { createClient, createDeserializer, Client } = require('minecraft-protocol');
const  pluginChannels  = require('minecraft-protocol/src/client/pluginChannels');
const { Duplex } = require('stream');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

Error.stackTraceLimit = 30;

class CustomDuplex extends Duplex {
    constructor(options, writeAction) {
        super(options);
        this.writeAction = writeAction;
    }

    _read() {}

    _write(chunk, encoding, callback) {
        this.writeAction(chunk);
        callback();
    }
}

/** @type {Map<string, { client: Client, serverClient: Client, duplexFromServer: any, duplexToServer: any, packets: {direction: string, data: Buffer}[], log: string, firstClientMessage: number | undefined }>} */
const connections = new Map();

parentPort.on('message', (message) => {
    switch (message.type) {
        case 'create_connection':
            handleCreateConnection(message.id, message.version, message.meta);
            break;
        case 'push_from_server':
            handlePushFromServer(message.id, message.packet);
            break;
        case 'push_from_client':
            handlePushFromClient(message.id, message.packet);
            break;
        case 'end_connection':
            handleEndConnection(message.id).finally(() => {
                parentPort.postMessage({
                    type: 'connection_ended',
                    id: message.id
                });
            });
            break;
        case 'log':
            handleLog(message.id, message.message);
            break;
        case 'channel_register':
            handleChannelRegister(message.id, message.data);
            break;
    }
});

function handleCreateConnection(id, version, meta) {
    let firstClientMessage

    const duplexFromServer = new CustomDuplex({}, (chunk) => {
    });
    /** @type {any} */
    const duplexToServer = new CustomDuplex({}, (chunk) => {
    });

    const client = createClient({
        stream: duplexFromServer,
        version: version,
        host: 'a',
        username: 'a',
        port: 0,
    });

    const serverClient = new Client(true, version)
    serverClient.setSocket(duplexToServer)

    client.on('compress', (data) => {
      serverClient.compressionThreshold = data.threshold
    })
    client.on('set_compression', (data) => {
      serverClient.compressionThreshold = data.threshold
    })
    client.on('success', (data) => {
        client.username = data.username
    })

    client.on('state', (state) => {
        serverClient.state = state
        // deserializer = createDeserializer({
        //     state,
        //     version,
        //     isServer: true,
        //     customPackets: {},
        //     noErrorLogging: true
        // })
    })

    client.on('packet', (data, packetMeta, buffer, fullBuffer) => {
        const connection = connections.get(id);
        if (connection) {
            logPacket(connection, true, packetMeta, data);
        }
    });

    serverClient.on('packet', (data, packetMeta, buffer, fullBuffer) => {
        firstClientMessage ??= Date.now()
        const connection = connections.get(id);
        if (connection) {
            logPacket(connection, false, packetMeta, data);
        }
    });

    client.emit('connect');

    connections.set(id, {
        client,
        duplexFromServer,
        serverClient,
        duplexToServer,
        packets: [],
        log: `{"minecraftVersion":"${version}"}\n# Connection id: ${id}. Started at: ${new Date().toISOString()}. User agent: ${meta.userAgent}\n`,
        get firstClientMessage() {
            return firstClientMessage
        }
    });
}

function handlePushFromServer(id, packet) {
    const connection = connections.get(id);
    if (connection) {
        connection.duplexFromServer.push(Buffer.from(packet));
        connection.packets.push({
            direction: 'S',
            data: packet
        });
    }
}

function handlePushFromClient(id, packet) {
    const connection = connections.get(id);
    if (connection) {
        connection.duplexToServer.push(Buffer.from(packet));

        connection.packets.push({
            direction: 'C',
            data: packet
        });
    }
}

const logPacket = (connection, isServer, { name }, params) => {
    const client = isServer ? connection.serverClient : connection.client;
    const string = `${isServer ? 'S' : 'C'} ${client.state}:${name} ${Date.now()} ${JSON.stringify(params)}\n`;
    connection.log += string;
}

function handleLog(id, message) {
    const connection = connections.get(id);
    if (connection) {
        connection.log += `# ${message}\n`;
    }
}

function handleChannelRegister(id, data) {
    const connection = connections.get(id);
    if (connection) {
        const { channel, parser } = data;
        // connection.client.registerChannel(channel, parser);

        // if (!connection.serverClient.registerChannel) {
        //     pluginChannels(connection.serverClient, { version: connection.client.version });
        // }
        // connection.serverClient.registerChannel(channel, parser, true);

        connection.log += `# Channel registered: ${channel}\n`;
    }
}

async function handleEndConnection(id) {
    const connection = connections.get(id);
    if (!connection) {
        throw new Error(`Connection ${id} not found`);
    }

    try {
        // End the client connection
        connection.client.end();

        // Create logs directory if it doesn't exist
        const logsDir = path.join(process.cwd(), 'logs');
        fs.mkdirSync(logsDir, { recursive: true });

        const elapsedSeconds = ((Date.now() - connection.firstClientMessage) / 1000).toFixed(0)

        const filename = `${id}-${connection.client.username}-${elapsedSeconds}s`;

        const logFile = path.join(logsDir, `${filename}.txt`);
        fs.writeFileSync(logFile, connection.log);

        // Write compressed log file
        const compressedFile = path.join(logsDir, `${filename}.gz`);
        const compressed = zlib.gzipSync(connection.log);
        fs.writeFileSync(compressedFile, compressed);

        const LOGS_LIMIT = 12
        const files = fs.readdirSync(logsDir);
        if (files.length > LOGS_LIMIT) {
            const sortedByDate = files.sort((a, b) => {
                const dateA = fs.statSync(path.join(logsDir, a)).birthtime;
                const dateB = fs.statSync(path.join(logsDir, b)).birthtime;
                return dateA.getTime() - dateB.getTime();
            });
            fs.unlinkSync(path.join(logsDir, sortedByDate[0]));
            fs.unlinkSync(path.join(logsDir, sortedByDate[1]));
            fs.unlinkSync(path.join(logsDir, sortedByDate[2]));
        }

        // Clean up the connection
        connections.delete(id);
    } catch (error) {
        console.error(`Error ending connection ${id}:`, error);
        throw error;
    }
}
