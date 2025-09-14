var net = require('net');
var http = require('http');
var crypto = require('crypto');
var express = require('express');
var expressWs = require('express-ws');
var bodyParser = require('body-parser');

const { handleNewConnection } = require('./mc.js')

function generateToken() {
	return crypto.randomBytes(32).toString('hex');
}

function checkTo(allowed, requested) {
	if (!(allowed instanceof Array)) {
		allowed = [allowed];
	}

	// For each rule
	for (var i = 0; i < allowed.length; i++) {
		var to = allowed[i];

		if ((to.host == requested.host || !to.host) && (to.port == requested.port || !to.port)) {
			if (to.blacklist) { // This item is blacklisted
				return false;
			} else { // Otheriwse, it's whitelisted
				return true;
			}
		}
	}

	// No rule found, access denied
	return false;
}

module.exports = function (options, connectionListener) {
	options = options || {};

	const myLog = options.log
		? console.log
		: function() {}

	const artificialDelay = options.artificialDelay || 0;

	function getArtificialDelay() {
		if (Array.isArray(artificialDelay) && artificialDelay.length === 2) {
			// Random delay between min and max values
			return Math.random() * (artificialDelay[1] - artificialDelay[0]) + artificialDelay[0];
		}
		return artificialDelay || 0;
	}

	function getMaxArtificialDelay() {
		if (Array.isArray(artificialDelay) && artificialDelay.length === 2) {
			// Use max value for connection closing operations
			return artificialDelay[1];
		}
		return artificialDelay || 0;
	}
	const maxPacketsPerSecond = options.maxPacketsPerSecond || 0;

	var app = express();
	var jsonParser = bodyParser.json();
	var urlRoot = options.urlRoot || '/api/vm/net';

	var server;
	if (options.server) {
		server = options.server;
	} else {
		server = http.createServer();
	}

	var sockets = {};
	var wsConnections = {};

	function handleTermination() {
		console.debug('Cleaning up...');
		// Send termination message to all active WebSocket connections
		Object.values(wsConnections).forEach(ws => {
			try {
				ws.send('proxy-shutdown:Proxy server is shutting down. Most probably it is being restarted so try to reconnect.', () => {});
				ws.close();
			} catch (err) {
				console.error('Error sending termination message:', err);
			}
		});
		process.exit(0);
	}

	process.on('SIGINT', handleTermination);
	process.on('SIGTERM', handleTermination);

	if (options.allowOrigin) {
		var allowOrigin = options.allowOrigin;
		if (typeof options.allowOrigin != 'string') {
			allowOrigin = (options.allowOrigin === true) ? '*' : '';
		}

		if (allowOrigin) {
			// Set Access-Control headers (CORS)
			app.use(function (req, res, next) {
				if (req.path.indexOf(urlRoot) !== 0) {
					next();
					return;
				}

				res.header('Access-Control-Allow-Origin', allowOrigin);

				if (req.method.toUpperCase() == 'OPTIONS') { // Preflighted requests
					res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
					res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-connection-id, x-minecraft-version, x-additional-info');

					res.header('Access-Control-Max-Age', 1728000); // Access-Control headers cached for 20 days
				}
				next();
			});
		}
	}

	const socketConnectTimeout = options.connectTimeout ?? options.timeout ?? 5000
	const connectionTimeout = options.connectionTimeout ?? options.timeout ?? 5000
	app.post(urlRoot + '/connect', jsonParser, async function (req, res) {
		var host = req.body.host,
			port = req.body.port;

		if (!host || !port) {
			res.status(400).send({
				code: 400,
				error: 'No host and port specified'
			});
			return;
		}
		let validateResult = ''
		if (options.validate) {
			try {
				validateResult = await options.validate(req, res)
				if (!validateResult) {
					if (!res.finished) {
						res.status(403).send({
							code: 403,
							error: 'You are not allowed to connect to this server'
						});
					}
					return;
				}
			} catch (err) {
				console.error('Error validating request:', err);
				if (!res.headersSent) {
					res.status(500).send({
						code: 500,
						error: 'Internal server error at validation step'
					});
				}
				return;
			}
		}

		if (options.to) {
			if (!checkTo(options.to, { host: host, port: port })) {
				res.status(403).send({
					code: 403,
					error: 'Destination not allowed'
				});
				return;
			}
		}

		var socket = net.connect({
			host: host,
			port: port,
			timeout: socketConnectTimeout
		}, function (err) {
			if (res.finished) {
				myLog("Socket connected after response closed");
				return
			}
			if (err) {
				res.status(500).send({
					code: 500,
					error: err
				});
				return;
			}

			// Generate a token for this connection
			var token = generateToken();
			sockets[token] = socket;

			// Remove the socket from the list when closed
			socket.on('end', function () {
				if (sockets[token]) {
					delete sockets[token];
				}
			});

			myLog('Connected to '+req.body.host+':'+req.body.port+' ('+token+')');

			var remote = socket.address();
			res.send({
				token: token,
				remote: remote,
				validateResult: validateResult
			});
		});

		if (req.headers['x-connection-id']) {
			const { onDataFromServer, onDataToServer, end, log, onChannelRegister } = handleNewConnection(
				req.headers['x-minecraft-version'] ?? '1.21.4',
				req.headers['x-connection-id'],
				{
					userAgent: req.headers['user-agent'] + ' ' + (req.headers['x-additional-info'] ?? ''),
					ip: req.ip,
					targetServer: host + ':' + port
				}
			)

			socket.on('data', (chunk) => {
				onDataFromServer(chunk)
			})
			const oldWrite = socket.write.bind(socket)
			socket.write = (...args) => {
				onDataToServer(args[0])
				return oldWrite(...args)
			}
			socket.on('end', () => {
				setTimeout(() => {
					end()
				})
			})
			socket.logWorker = log
			socket.onChannelRegister = onChannelRegister
		} else {
			console.warn('WARN: No connection ID provided for connection')
		}

		let start = Date.now()
		// Handle the 'timeout' event
		socket.on('timeout', function () {
			myLog('Socket timed out after '+((Date.now() - start)/1000)+' seconds');
			if (!res.finished) {
				res.status(504).send({
					code: 504,
					error: `Socket timed out. Ensure the server ${host} is reachable and the port ${port} is open.`
				});
			}
			socket.end(); // End the connection
		});
		socket.on('error', function (err) {
			if (res.finished) {
				myLog("Socket error after response closed: "+err);
				return;
			}
			res.status(502).send({
				code: 502,
				error: 'Socket error: '+err.code,
				details: err
			});
		});
		if (connectionListener) {
			connectionListener(socket)
		}
	});

	app.get(urlRoot + '/connect', function (req, res) {
		try {
			const startTime = process.hrtime()
			res.json({
				code: 200,
				description: 'A proxy server for Minecraft web clients',
				time: Date.now(),
				processingTime: process.hrtime(startTime)[1] / 1000000
			})
		} catch (err) {
			console.error('Error handling connect request:', err)
		}
	})

	var wss = expressWs(app, server);

	// Add dedicated WebSocket ping endpoint
	app.ws(urlRoot + '/ping', function (ws, req) {
		try {
			myLog('Ping WebSocket connection opened')
			if (options.onPing) {
				options.onPing(req)
			}

			ws.on('message', function (data) {
				if (typeof data === 'string' && data.startsWith('ping:')) {
					const startTime = process.hrtime()
					const pingId = data.slice('ping:'.length)
					ws.send('pong:' + pingId + ':' + (process.hrtime(startTime)[1] / 1000000), () => { })
				}
			})

			// Clean up on connection close
			ws.on('close', function () {
				myLog('Ping WebSocket connection closed')
			})
		} catch (err) {
			console.error('Error handling ping WebSocket connection:', err)
		}
	});

	app.ws(urlRoot + '/socket', function (/** @type {import('ws').WebSocket} */ws, req) {
		var token = req.query.token;

		if (!sockets[token]) {
			console.warn('WARN: Unknown TCP connection with token "'+token+'"');
			ws.close();
			return;
		}

		var socket = sockets[token];
		socket.setTimeout(connectionTimeout);
		wsConnections[token] = ws; // Track WebSocket connection

		myLog('Forwarding socket with token '+token);

		// Packet rate monitoring
		let packetsLastSecond = { fromClient: 0, fromServer: 0 };
		let lastCheck = Date.now();

		function checkPacketRate() {
			const now = Date.now();
			const elapsed = now - lastCheck;
			if (elapsed >= 1000 && maxPacketsPerSecond > 0) {
				const clientRate = (packetsLastSecond.fromClient * 1000) / elapsed;
				const serverRate = (packetsLastSecond.fromServer * 1000) / elapsed;

				if (clientRate > maxPacketsPerSecond) {
					myLog(`Warning: Client->Server packet rate (${Math.round(clientRate)}/s) exceeds limit of ${maxPacketsPerSecond}/s`);
				}
				if (serverRate > maxPacketsPerSecond) {
					myLog(`Warning: Server->Client packet rate (${Math.round(serverRate)}/s) exceeds limit of ${maxPacketsPerSecond}/s`);
				}

				packetsLastSecond.fromClient = 0;
				packetsLastSecond.fromServer = 0;
				lastCheck = now;
			}
		}

		ws.on('message', function (data) {
			if (typeof data === 'string' && data.startsWith('ping:')) {
				ws.send('pong:' + data.slice('ping:'.length), () => {});
				return
			}
			if (typeof data === 'string' && data.startsWith('custom-channel-register:')) {
				try {
					const parsed = JSON.parse(data.slice('custom-channel-register:'.length))
					socket.onChannelRegister?.(parsed)
				} catch (err) {
					console.log('Error parsing custom-channel-register:', err)
				}
				return
			}

			packetsLastSecond.fromClient++;
			checkPacketRate();

			const delay = getArtificialDelay();
			if (delay > 0) {
				setTimeout(() => {
					socket.write(data, 'binary', function () {
						//myLog('Sent: ', data.toString());
					});
				}, delay);
			} else {
				socket.write(data, 'binary', function () {
					//myLog('Sent: ', data.toString());
				});
			}
		});

		socket.on('data', function (chunk) {
			//myLog('Received: ', chunk.toString());
			packetsLastSecond.fromServer++;
			checkPacketRate();

			const delay = getArtificialDelay();
			if (delay > 0) {
				setTimeout(() => {
					ws.send(chunk, { binary: true }, function (err) {});
				}, delay);
			} else {
				if (ws.readyState !== 1) {
					// console.log('Wrong readyState!', ws.readyState)
					socket.logWorker?.('Wrong readyState!', ws.readyState)
				}

				ws.send(chunk, { binary: true }, function (err) {});
			}
		});

		let reasonSent = false;
		socket.on('timeout', function () {
			if (!reasonSent) {
				ws.send('proxy-shutdown:Connection timed out. No packets were sent or received from either side in '+connectionTimeout+'ms.', () => {});
				reasonSent = true;
			}
			socket.logWorker?.('TCP connection timed out');
		});

		socket.on('error', function (err) {
			if (!reasonSent) {
				const message = err.code === 'EADDRNOTAVAIL' ? 'Minecraft server is not reachable anymore.' : 'Issue with the connection to the Minecraft server: '+err.message;
				ws.send('proxy-shutdown:'+message, () => {});
				reasonSent = true;
			}
			socket.logWorker?.('TCP connection error: '+err.message);
		});

		socket.on('close', function () {
			// todo let client know of errors somehow
			myLog('TCP connection closed by remote ('+token+')');
			if (!reasonSent) {
				ws.send('proxy-shutdown:Minecraft server closed the connection.', () => {});
			}
			const delay = getMaxArtificialDelay();
			if (delay > 0) {
				setTimeout(() => {
					ws.close();
				}, delay);
			} else {
				ws.close();
			}
			socket.logWorker?.('TCP connection closed by remote');
			delete wsConnections[token]; // Clean up WebSocket connection
		});
		ws.on('close', function () {
			const delay = getMaxArtificialDelay();
			if (delay > 0) {
				setTimeout(() => {
					socket.end();
				}, delay);
			} else {
				socket.end();
			}
			socket.logWorker?.('Websocket connection closed');
			myLog('Websocket connection closed ('+token+')');
			delete wsConnections[token]; // Clean up WebSocket connection
		});
	});

	app.on('mount', function (parentApp) {
		// @see https://github.com/strongloop/express/blob/master/lib/application.js#L615
		parentApp.listen = function listen() {
			server.addListener('request', this);
			return server.listen.apply(server, arguments);
		};
	});

	return app;
};
