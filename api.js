var net = require('net');
var http = require('http');
var crypto = require('crypto');
var express = require('express');
var expressWs = require('express-ws');
var bodyParser = require('body-parser');

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
					res.header('Access-Control-Allow-Headers', 'Content-Type');

					res.header('Access-Control-Max-Age', 1728000); // Access-Control headers cached for 20 days
				}
				next();
			});
		}
	}

	app.post(urlRoot + '/connect', jsonParser, function (req, res) {
		var host = req.body.host,
			port = req.body.port;

		if (!host || !port) {
			res.status(400).send({
				code: 400,
				error: 'No host and port specified'
			});
			return;
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
			port: port
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
				remote: remote
			});
		});
		socket.setTimeout(options.timeout ?? 5000); // 5000 milliseconds = 5 seconds

		// Handle the 'timeout' event
		socket.on('timeout', function () {
			myLog('Socket timed out');
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

	var wss = expressWs(app, server);

	app.ws(urlRoot + '/socket', function (ws, req) {
		var token = req.query.token;

		if (!sockets[token]) {
			console.warn('WARN: Unknown TCP connection with token "'+token+'"');
			ws.close();
			return;
		}

		var socket = sockets[token];
		//delete sockets[token];

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
				ws.send('pong:' + data.slice('ping:'.length));
				return;
			}

			packetsLastSecond.fromClient++;
			checkPacketRate();

			if (artificialDelay > 0) {
				setTimeout(() => {
					socket.write(data, 'binary', function () {
						//myLog('Sent: ', data.toString());
					});
				}, artificialDelay);
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

			if (artificialDelay > 0) {
				setTimeout(() => {
					ws.send(chunk, { binary: true }, function (err) {});
				}, artificialDelay);
			} else {
				ws.send(chunk, { binary: true }, function (err) {});
			}
		});

		socket.on('close', function () {
			// todo let client know of errors somehow
			myLog('TCP connection closed by remote ('+token+')');
			ws.close();
		});
		ws.on('close', function () {
			socket.end();
			myLog('Websocket connection closed ('+token+')');
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
