var stream = require('stream');
var util = require('util');
var timers = require('timers');
var http = require('http');

var debug = util.debuglog('net');

var defaultProxy = {
	protocol: (window.location.protocol == 'https:') ? 'wss' : 'ws',
	requestProtocol: '',
	hostname: window.location.hostname,
	port: window.location.port,
	path: '/api/vm/net',
	headers: {}
};
var proxy = { ...defaultProxy }
function getProxy() {
	return proxy;
}
function getProxyHost() {
	var host = getProxy().hostname;
	if (getProxy().port) {
		host += ':'+getProxy().port;
	}
	return host;
}
function getProxyOrigin() {
	if (getProxy().requestProtocol) {
		proxy.protocol = getProxy().requestProtocol === 'https:' ? 'wss' : 'ws'
	}
	return getProxy().protocol + '://' + getProxyHost();
}
exports.setProxy = function (options) {
	proxy = { ...defaultProxy }
	options = options || {};

	let { hostname } = options
	if (hostname) {
		let requestProtocol
		if (hostname.startsWith('http://')) {
			requestProtocol = 'http:';
			hostname = hostname.slice(7);
		} else if (hostname.startsWith('https://')) {
			requestProtocol = 'https:';
			hostname = hostname.slice(8);
		}
		proxy.requestProtocol = requestProtocol;
		proxy.hostname = hostname;
	}
	if (options.port) {
		proxy.port = options.port;
	}
	if (options.path) {
		proxy.path = options.path;
	}
	if (options.headers) {
		proxy.headers = options.headers;
	}
};

exports.createServer = function () {
	throw new Error('Cannot create server in a browser');
};

exports.connect = exports.createConnection = function (/* options, connectListener */) {
	var args = normalizeConnectArgs(arguments);
	debug('createConnection', args);
	var s = new Socket(args[0]);
	return Socket.prototype.connect.apply(s, args);
};

function toNumber(x) { return (x = Number(x)) >= 0 ? x : false; }

function isPipeName(s) {
	return util.isString(s) && toNumber(s) === false;
}

// Returns an array [options] or [options, cb]
// It is the same as the argument of Socket.prototype.connect().
function normalizeConnectArgs(args) {
	var options = {};
	if (util.isObject(args[0])) {
		// connect(options, [cb])
		options = args[0];
	} else if (isPipeName(args[0])) {
		// connect(path, [cb]);
		options.path = args[0];
	} else {
		// connect(port, [host], [cb])
		options.port = args[0];
		if (util.isString(args[1])) {
			options.host = args[1];
		}
	}
	var cb = args[args.length - 1];
	return util.isFunction(cb) ? [options, cb] : [options];
}
exports._normalizeConnectArgs = normalizeConnectArgs;

function Socket(options) {
	if (!(this instanceof Socket)) return new Socket(options);

	this._connecting = false;
	this._host = null;

	if (util.isNumber(options))
		options = { fd: options }; // Legacy interface.
	else if (util.isUndefined(options))
		options = {};

	stream.Duplex.call(this, options);

	// these will be set once there is a connection
	this.readable = this.writable = false;

	// handle strings directly
	this._writableState.decodeStrings = false;

	// default to *not* allowing half open sockets
	this.allowHalfOpen = options && options.allowHalfOpen || false;

	// Set default timeout to 10 seconds if not specified
	this._wsTimeout = options && options.wsTimeout || 10000;
}
util.inherits(Socket, stream.Duplex);

exports.Socket = Socket;
exports.Stream = Socket; // Legacy naming.

Socket.prototype.listen = function () {
	throw new Error('Cannot listen in a browser');
};

Socket.prototype.setTimeout = function (msecs, callback) {
	if (msecs > 0 && isFinite(msecs)) {
		timers.enroll(this, msecs);
		//timers._unrefActive(this);
		if (callback) {
			this.once('timeout', callback);
		}
	} else if (msecs === 0) {
		timers.unenroll(this);
		if (callback) {
			this.removeListener('timeout', callback);
		}
	}
};

Socket.prototype._onTimeout = function () {
	debug('_onTimeout');
	this.emit('timeout');
};

Socket.prototype.setNoDelay = function (enable) {};
Socket.prototype.setKeepAlive = function (setting, msecs) {};

Socket.prototype.address = function () {
	return {
		address: this.remoteAddress,
		port: this.remotePort,
		family: this.remoteFamily
	};
};

Object.defineProperty(Socket.prototype, 'readyState', {
	get: function() {
		if (this._connecting) {
			return 'opening';
		} else if (this.readable && this.writable) {
			return 'open';
		} else if (this.readable && !this.writable) {
			return 'readOnly';
		} else if (!this.readable && this.writable) {
			return 'writeOnly';
		} else {
			return 'closed';
		}
	}
});

Socket.prototype.bufferSize = undefined;

Socket.prototype._read = function () {};

Socket.prototype.end = function(data, encoding) {
	stream.Duplex.prototype.end.call(this, data, encoding);
	this.writable = false;

	if (this._ws) {
		this._ws.close();
	}

	// just in case we're waiting for an EOF.
	if (this.readable && !this._readableState.endEmitted)
		this.read(0);
	else
		maybeDestroy(this);
};

// Call whenever we set writable=false or readable=false
function maybeDestroy(socket) {
	if (!socket.readable &&
		!socket.writable &&
		!socket.destroyed &&
		!socket._connecting &&
		!socket._writableState.length) {
		socket.destroy();
	}
}

Socket.prototype.destroySoon = function() {
	if (this.writable)
		this.end();
	if (this._writableState.finished)
		this.destroy();
	else
		this.once('finish', this.destroy);
};

Socket.prototype.destroy = function(exception) {
	debug('destroy', exception);

	if (this.destroyed) {
		return;
	}

	this._connecting = false;

	this.readable = this.writable = false;

	timers.unenroll(this);

	debug('close');

	this.destroyed = true;
};

Socket.prototype.remoteAddress = null;
Socket.prototype.remoteFamily = null;
Socket.prototype.remotePort = null;

// Used for servers only - not here
Socket.prototype.localAddress = null;
Socket.prototype.localPort = null;

Socket.prototype.bytesRead = 0;
Socket.prototype.bytesWritten = 0;

Socket.prototype._write = function (data, encoding, cb) {
	var self = this;
	cb = cb || function () {};

	// If we are still connecting, then buffer this for later.
	// The Writable logic will buffer up any more writes while
	// waiting for this one to be done.
	if (this._connecting) {
		this._pendingData = data;
		this._pendingEncoding = encoding;
		this.once('connect', function() {
			this._write(data, encoding, cb);
		});
		return;
	}
	this._pendingData = null;
	this._pendingEncoding = '';

	if (encoding == 'binary' && typeof data == 'string') { //TODO: maybe apply this for all string inputs?
		// Setting encoding is very important for binary data - otherwise the data gets modified
		data = new Buffer(data, encoding);
	}

	// Send the data
	this._ws.send(data);

	process.nextTick(function () {
		//console.log('[tcp] sent: ', data.toString(), data.length);
		self.bytesWritten += data.length;
		cb();
	});
};

Socket.prototype.write = function(chunk, encoding, cb) {
	if (!util.isString(chunk) && !util.isBuffer(chunk))
		throw new TypeError('invalid data');
	return stream.Duplex.prototype.write.apply(this, arguments);
};

Socket.prototype.connect = function(options, cb) {
	var self = this;

	if (!util.isObject(options)) {
		// Old API:
		// connect(port, [host], [cb])
		// connect(path, [cb]);
		var args = normalizeConnectArgs(arguments);
		return Socket.prototype.connect.apply(this, args);
	}

	cb = cb || function () {};

	if (this.write !== Socket.prototype.write)
		this.write = Socket.prototype.write;

	if (options.path) {
		throw new Error('options.path not supported in the browser');
	}

	self._connecting = true;
	self.writable = true;
	self._host = options.host;

	let timedOut = false
	let timeout
	var req = http.request({
		hostname: getProxy().hostname,
		port: getProxy().port,
		path: getProxy().path + '/connect',
		protocol: getProxy().requestProtocol,
		method: 'POST',
		withCredentials: false,
		headers: {
			'Content-Type': 'application/json',
			...getProxy().headers
		}
	}, function (res) {
		if (timedOut) return
		clearTimeout(timeout)

		var json = '';
		res.on('data', function (buf) {
			json += buf;
		});
		res.on('end', function () {
			var data = null;
			try {
				data = JSON.parse(json);
			} catch (e) {
				data = {
					code: res.statusCode,
					error: json
				};
			}

			if (data.error !== undefined) {
				let errorMessage = 'Cannot open TCP connection ['+res.statusCode+']: '+JSON.stringify(data.error)
				if (res.statusCode === 0) {
					errorMessage = `Cannot reach the proxy server ${getProxy().hostname}:${getProxy().port}`
				} else if (res.statusCode === 404) {
					errorMessage = 'Cannot find the proxy server (404). Check proxy ip you entered.'
				}
				if (data.error.code === 'ENOTFOUND') {
					errorMessage = 'Cannot find the server. Check server ip you entered.'
				}
				self.emit('error', errorMessage);
				self.destroy();
				return;
			}

			self.remoteAddress = data.remote.address;
			self.remoteFamily = data.remote.family;
			self.remotePort = data.remote.port;

			self._connectWebSocket(data.token, function (err) {
				if (err) {
					cb(err);
					return;
				}

				cb();
			});
		});
	});
	timeout = setTimeout(() => {
		if (!timedOut) {
			timedOut = true
			req.xhr.abort()
			self.emit('error', `Timeout for connecting to proxy ${getProxy().hostname}:${getProxy().port}. Make sure it is reachable: try opening it in your browser or use alternative proxy server.`)
			self.destroy()
		}
	}, 10_000)

	req.setHeader('Content-Type', 'application/json');
	// Set additional headers from proxy configuration
	Object.entries(getProxy().headers).forEach(([key, value]) => {
		req.setHeader(key, value);
	});
	req.write(JSON.stringify(options));
	req.end();

	return this;
};

Socket.prototype._connectWebSocket = function (token, cb) {
	var self = this;

	if (self._ws) {
		process.nextTick(function () {
			cb();
		});
		return;
	}

	this._ws = new WebSocket(getProxyOrigin() + getProxy().path + '/socket?token='+token);
	this._handleWebsocket();

	if (cb) {
		self.on('connect', cb);
	}
};

Socket.prototype.handleStringMessage = function (message) {
	return false
}

Socket.prototype._handleWebsocket = function () {
	var self = this;

	// Add connection timeout
	let timedOut = false;
	const timeout = setTimeout(() => {
		if (!timedOut) {
			timedOut = true;
			if (self._ws) {
				self._ws.close();
			}
			const error = `Proxy server is reachable, but the WebSocket connection timed out after ${this._wsTimeout / 1000} seconds. Possible reasons:
1. Most probably the proxy server (${getProxy().hostname}:${getProxy().port}) is misconfigured and not accepting WebSocket connections.
2. Your browser or network is blocking WebSocket connections.`;
			self.emit('error', error);
		}
	}, this._wsTimeout);

	this._ws.addEventListener('open', function () {
		if (timedOut) return;
		clearTimeout(timeout);
		//console.log('TCP OK');

		self._connecting = false;
		self.readable = true;

		self.emit('connect');

		self.read(0);
	});
	this._ws.addEventListener('error', function (e) {
		if (timedOut) return;
		clearTimeout(timeout);
		// `e` doesn't contain anything useful (https://developer.mozilla.org/en/docs/WebSockets/Writing_WebSocket_client_applications#Connection_errors)
		console.warn('TCP error', e);
		self.emit('error', 'An error occurred with the WebSocket connection. Please check your network connection and proxy server status.');
	});
	let reading = false
	this._ws.addEventListener('message', function (e) {
		var contents = e.data;

		var gotBuffer = function (buffer) {
			//console.log('[tcp] received: ' + buffer.toString(), buffer.length);
			self.bytesRead += buffer.length;
			self.push(buffer);
		};

		if (typeof contents == 'string') {
			if (contents.startsWith('pong:')) {
				self.emit('pong', contents.slice('pong:'.length));
				return
			}
			if (self.handleStringMessage(contents)) {
				var buffer = new Buffer(contents);
				gotBuffer(buffer);
			}
		} else if (window.Blob && contents instanceof Blob) {
			var fileReader = new FileReader();
			let resolveReading
			reading = new Promise((resolve) => {
				resolveReading = resolve
			})

			fileReader.addEventListener('load', function (e) {
				var buf = fileReader.result;
				var arr = new Uint8Array(buf);
				gotBuffer(new Buffer(arr));
				resolveReading()
				reading = false
			});
			fileReader.addEventListener('error', function (e) {
				console.warn('Cannot read TCP stream: file reader error', e);
				resolveReading()
				reading = false
			});
			fileReader.readAsArrayBuffer(contents);
		} else {
			console.warn('Cannot read TCP stream: unsupported message type', contents);
		}
	});
	this._ws.addEventListener('close', async function () {
		if (self.readyState == 'open') {
			//console.log('TCP closed');
			await reading
			self.destroy();
		}
	});
};

exports.isIP = function (input) {
	if (exports.isIPv4(input)) {
		return 4;
	} else if (exports.isIPv6(input)) {
		return 6;
	} else {
		return 0;
	}
};
exports.isIPv4 = function(input) {
	return /^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/.test(input);
};
exports.isIPv6 = function(input) {
	return /^(([0-9A-Fa-f]{1,4}:){7}([0-9A-Fa-f]{1,4}|:))|(([0-9A-Fa-f]{1,4}:){6}(:[0-9A-Fa-f]{1,4}|((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){5}(((:[0-9A-Fa-f]{1,4}){1,2})|:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){4}(((:[0-9A-Fa-f]{1,4}){1,3})|((:[0-9A-Fa-f]{1,4})?:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){3}(((:[0-9A-Fa-f]{1,4}){1,4})|((:[0-9A-Fa-f]{1,4}){0,2}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){2}(((:[0-9A-Fa-f]{1,4}){1,5})|((:[0-9A-Fa-f]{1,4}){0,3}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){1}(((:[0-9A-Fa-f]{1,4}){1,6})|((:[0-9A-Fa-f]{1,4}){0,4}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(:(((:[0-9A-Fa-f]{1,4}){1,7})|((:[0-9A-Fa-f]{1,4}){0,5}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))$/.test(input);
};
