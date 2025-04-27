//@ts-check
const { Worker } = require('worker_threads');
const path = require('path');

const workers = new Map();
const SHUTDOWN_TIMEOUT = 5000; // 5 seconds timeout for graceful shutdown

module.exports.handleNewConnection = (/** @type {string} */version, /** @type {string} */id = Math.random().toString(36).substring(7), /** @type {any} */meta) => {
    const worker = new Worker(path.join(__dirname, 'worker.js'));
    workers.set(id, worker);

    worker.on('message', (message) => {
        switch (message.type) {
            case 'packet_from_server':
                // Handle packet from server
                break;
            case 'packet_log':
                // Handle packet log
                break;
            case 'connection_ended':
                // Worker has finished processing, safe to terminate
                worker.terminate();
                workers.delete(id);
                break;
        }
    });

    worker.on('error', (error) => {
        console.error(`Worker ${id} error:`, error);
    });

    worker.on('exit', (code) => {
        if (code !== 0) {
            console.error(`Worker ${id} exited with code ${code}`);
        }
        workers.delete(id);
    });

    worker.postMessage({
        type: 'create_connection',
        id,
        version,
        meta
    });

    return {
        onDataFromServer: (data) => {
            worker.postMessage({
                type: 'push_from_server',
                id,
                packet: data
            });
        },
        onDataToServer: (data) => {
            worker.postMessage({
                type: 'push_from_client',
                id,
                packet: data
            });
        },
        log(message) {
            worker.postMessage({
                type: 'log',
                id,
                message
            });
        },
        onChannelRegister(data) {
            worker.postMessage({
                type: 'channel_register',
                id,
                data
            });
        },
        end() {
            return new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    if (workers.has(id)) {
                        console.warn(`Worker ${id} shutdown timed out after ${SHUTDOWN_TIMEOUT}ms`);
                        worker.terminate();
                        workers.delete(id);
                        resolve();
                    }
                }, SHUTDOWN_TIMEOUT);

                worker.postMessage({
                    type: 'end_connection',
                    id
                });

                // The worker will send a 'connection_ended' message when it's done
                // This will be handled by the message handler above
                // The timeout ensures we don't wait forever if something goes wrong
            });
        }
    }
}
