"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
const node_crypto_1 = require("node:crypto");
const utils = __importStar(require("@iobroker/adapter-core"));
/** Delay before a new connection attempt after the socket closed. */
const RECONNECT_DELAY_MS = 60_000;
/** Interval for client KeepAlive messages if the server does not request one. */
const DEFAULT_KEEPALIVE_MS = 30_000;
/** Lower limit for the poll interval. */
const MIN_POLL_SECONDS = 5;
/** One day in ms. */
const DAY_MS = 24 * 60 * 60 * 1000;
/** Timeout for REST calls to the server. */
const HTTP_TIMEOUT_MS = 10_000;
/** Interval for reading the server information (version, update, restart). */
const SERVER_INFO_INTERVAL_MS = 10 * 60_000;
/** 1 s in Emby ticks (1 tick = 100 ns). */
const TICKS_PER_SECOND = 10_000_000;
/** Device ids matching this pattern are used as object id directly, all others are hashed. */
const PLAIN_DEVICE_ID = /^[A-Za-z0-9_-]{1,40}$/;
/** Ids of the adapter's own channels; a device must never get one of them. */
const RESERVED_IDS = new Set(['info', 'server']);
/** Valid values of common.type; versions <= 1.3.0 created buttons with type "button" */
const STATE_TYPES = new Set(['number', 'string', 'boolean', 'array', 'object', 'mixed', 'json']);
/**
 * Simple remote commands: state id below `<session>.` → REST path below `/Sessions/<session>/`.
 */
const SIMPLE_COMMANDS = {
    'command.toggleplay': 'Playing/PlayPause',
    'command.play': 'Playing/Unpause',
    'command.pause': 'Playing/Pause',
    'command.mute': 'Command/Mute',
    'command.unmute': 'Command/Unmute',
    'command.togglemute': 'Command/ToggleMute',
    'command.goHome': 'Command/GoHome',
    'command.goToSearch': 'Command/GoToSearch',
    'command.back': 'Command/Back',
    'command.stop': 'Playing/Stop',
    'command.next': 'Playing/NextTrack',
    'command.previous': 'Playing/PreviousTrack',
};
/**
 * Builds a read-only state definition.
 *
 * @param name name
 * @param type common.type
 * @param role role
 */
function roState(name, type, role = 'state') {
    return { type: 'state', common: { name, role, type, read: true, write: false }, native: {} };
}
/**
 * Builds a button definition.
 *
 * @param name name
 * @param role button role (see ioBroker state roles, e.g. button.stop)
 */
function button(name, role = 'button') {
    return { type: 'state', common: { name, role, type: 'boolean', read: false, write: true }, native: {} };
}
/**
 * Object name for log messages. The adapter writes plain strings, but common.name may be translated.
 *
 * @param name common.name
 */
function nameOf(name) {
    return typeof name === 'string' ? name : JSON.stringify(name);
}
/**
 * Formats the expected end time as HH:MM.
 *
 * @param msFromNow remaining playback time in ms
 */
function formatEndTime(msFromNow) {
    const end = new Date(Date.now() + msFromNow);
    return `${String(end.getHours()).padStart(2, '0')}:${String(end.getMinutes()).padStart(2, '0')}`;
}
class EmbyAdapter extends utils.Adapter {
    connection = null;
    stopping = false;
    retryTimer = null;
    keepAliveTimer = null;
    pollTimer = null;
    serverInfoTimer = null;
    /** Device key → duration of the current item in ticks (needed for command.seek in %). */
    durationOf = new Map();
    /** Serialises processing of Sessions messages so slow object creation cannot overlap. */
    sessionQueue = Promise.resolve();
    /** Device key → signature (name, remote control, supported commands) of the objects created during this run. */
    createdDevices = new Map();
    /** Device keys reported by the server in the last Sessions message. */
    activeDevices = new Set();
    /** Device key → current Emby session id (needed for the REST commands). */
    sessionOf = new Map();
    /** Sessions of ioBroker connections already checked for objects created by versions <= 2.x */
    checkedOwnSessions = new Set();
    initialSessionsLoaded = false;
    /** API key, resolved once at start (see migratePlainApiKey). */
    apiKey = '';
    laststates = {};
    timeoutplays = {};
    timeoutstates = {};
    timeoutstarted = {};
    constructor(options = {}) {
        super({ ...options, name: 'emby' });
        this.on('ready', () => {
            this.onReady().catch(e => this.log.error(`Startup failed: ${e.message}`));
        });
        this.on('stateChange', (id, state) => this.onStateChange(id, state));
        this.on('objectChange', (id, obj) => this.onObjectChange(id, obj));
        this.on('unload', callback => this.onUnload(callback));
        this.on('message', obj => this.onMessage(obj));
    }
    /** Adapter start. */
    async onReady() {
        await this.setStateAsync('info.connection', false, true);
        // the key is copied once, this.config.apikey is not read again (see migratePlainApiKey)
        this.apiKey = this.config.apikey || '';
        await this.migrateStoredDevices();
        if (await this.migratePlainApiKey()) {
            // @KI_HINWEIS: updateConfig() makes the js-controller restart the instance right away; a
            // connection started now would be cut off, and its timers would be created while stopping.
            return;
        }
        if (!this.config.ip || this.config.ip === '0.0.0.0:8096') {
            this.log.error('No server address configured (IP:Port).');
            return;
        }
        if (!this.apiKey) {
            this.log.warn('There is no ApiKey. You can receive information but cannot control clients.');
        }
        this.subscribeStates('*');
        this.subscribeObjects('*');
        this.tryConnect();
        this.startPolling();
    }
    /**
     * Called when the adapter shuts down. The callback has to be called under any circumstances.
     *
     * @param callback done callback
     */
    onUnload(callback) {
        this.stopping = true;
        try {
            this.stopTimers();
            Object.keys(this.timeoutplays).forEach(id => this.clearTimeout(this.timeoutplays[id]));
            if (this.connection) {
                if (this.connection.readyState === WebSocket.OPEN) {
                    this.connection.send(JSON.stringify({ MessageType: 'SessionsStop', Data: '' }));
                }
                this.connection.close();
                this.connection = null;
            }
            void this.setState('info.connection', false, true);
        }
        catch {
            // ignore, we are shutting down anyway
        }
        finally {
            callback();
        }
    }
    /** Stops reconnect, keepalive and poll timers. */
    stopTimers() {
        if (this.retryTimer) {
            this.clearTimeout(this.retryTimer);
            this.retryTimer = null;
        }
        if (this.keepAliveTimer) {
            this.clearInterval(this.keepAliveTimer);
            this.keepAliveTimer = null;
        }
        if (this.pollTimer) {
            this.clearInterval(this.pollTimer);
            this.pollTimer = null;
        }
        if (this.serverInfoTimer) {
            this.clearInterval(this.serverInfoTimer);
            this.serverInfoTimer = null;
        }
    }
    /** Base URL of the server for REST calls (honours the SSL option), e.g. http://192.168.0.100:8096 */
    baseUrl() {
        return `${this.config.isSSL ? 'https' : 'http'}://${this.config.ip}`;
    }
    /**
     * Sends a request to the server. The API key is sent as header, never in the URL,
     * so it cannot end up in logs.
     *
     * @param method HTTP method
     * @param path path below the server root, e.g. `Sessions/<id>/Playing/Pause`
     * @param body JSON body
     * @returns response (status 2xx)
     */
    async apiRequest(method, path, body) {
        const res = await fetch(`${this.baseUrl()}/${path}`, {
            method,
            headers: {
                'Content-Type': 'application/json',
                'X-Emby-Token': this.apiKey,
                // same DeviceId as the WebSocket, otherwise the server registers every REST call as an extra client
                'X-Emby-Authorization': `MediaBrowser Client="ioBroker", Device="ioBroker", DeviceId="${this.ownDeviceId()}", Version="${this.version}"`,
            },
            body: body !== undefined ? JSON.stringify(body) : undefined,
            signal: AbortSignal.timeout(HTTP_TIMEOUT_MS),
        });
        if (!res.ok) {
            throw new Error(`HTTP ${res.status} ${res.statusText}`);
        }
        return res;
    }
    /**
     * Sends a POST request to the server.
     *
     * @param path path below the server root
     * @param body JSON body
     */
    async apiPost(path, body) {
        await this.apiRequest('POST', path, body);
    }
    /**
     * Queues a list of sessions for processing (WebSocket push or REST poll).
     *
     * @param data session list
     */
    queueSessions(data) {
        const list = Array.isArray(data) ? data : [];
        this.sessionQueue = this.sessionQueue
            .then(() => this.handleSessions(list))
            .catch(err => this.log.warn(`Error processing sessions: ${err.message}`));
    }
    /**
     * Reads the current sessions via REST and runs the device cleanup afterwards. Needs an API key.
     *
     * @param quiet log failures only on debug level (periodic poll)
     */
    async refreshSessions(quiet = false) {
        if (!this.apiKey) {
            return;
        }
        try {
            const res = await this.apiRequest('GET', 'Sessions');
            this.queueSessions(await res.json());
            this.sessionQueue = this.sessionQueue
                .then(() => this.cleanupDevices())
                .catch(err => this.log.warn(`Error during device cleanup: ${err.message}`));
        }
        catch (e) {
            this.log[quiet ? 'debug' : 'warn'](`Could not read sessions: ${e.message}`);
        }
    }
    /**
     * Reads the device ids registered on the server (Dashboard > Devices).
     *
     * @returns device ids, null if unknown (request failed or empty list)
     */
    async getRegisteredDeviceIds() {
        try {
            const res = await this.apiRequest('GET', 'Devices');
            const data = (await res.json());
            const items = Array.isArray(data) ? data : data && Array.isArray(data.Items) ? data.Items : [];
            const ids = new Set();
            for (const item of items) {
                // Emby: ReportedDeviceId = DeviceId of the session, Id = internal number; Jellyfin: Id
                for (const id of [item.ReportedDeviceId, item.Id]) {
                    if (id !== undefined && id !== null && id !== '') {
                        ids.add(String(id));
                    }
                }
            }
            // an empty list is treated as unknown so a server problem can never wipe all devices
            return ids.size > 0 ? ids : null;
        }
        catch (e) {
            this.log.debug(`Could not read devices: ${e.message}`);
            return null;
        }
    }
    /** Device objects of all sessions below the adapter namespace. */
    async getSessionDevices() {
        const view = await this.getObjectViewAsync('system', 'device', {
            startkey: `${this.namespace}.`,
            endkey: `${this.namespace}.香`,
        });
        return view.rows.map(row => ({ sid: row.id.substring(this.namespace.length + 1), obj: row.value }));
    }
    /**
     * Removes the objects of inactive clients according to the cleanup settings:
     * clients deleted in the Emby dashboard and/or clients without activity for the configured days.
     * Sessions the server currently reports are never touched.
     */
    async cleanupDevices() {
        const removeDeleted = this.config.removeDeletedDevices !== false;
        const days = Number(this.config.removeInactiveDays) || 0;
        if (!removeDeleted && days <= 0) {
            return;
        }
        const registered = removeDeleted ? await this.getRegisteredDeviceIds() : null;
        for (const { sid, obj } of await this.getSessionDevices()) {
            if (this.activeDevices.has(sid)) {
                continue;
            }
            const deviceId = obj?.native?.deviceId || '';
            const name = obj?.common ? obj.common.name : sid;
            let reason = '';
            if (registered && deviceId && !registered.has(deviceId)) {
                reason = 'it was removed in Emby';
            }
            else if (days > 0) {
                const last = await this.getLastActivity(sid);
                if (last > 0 && Date.now() - last > days * DAY_MS) {
                    reason = `it was inactive for more than ${days} days`;
                }
            }
            if (reason) {
                this.log.info(`Removing objects of client "${nameOf(name)}" (${sid}) because ${reason}`);
                await this.deleteDeviceObjects(sid);
            }
        }
    }
    /**
     * Time of the last activity of a session from its states.
     *
     * @param sid session id
     * @returns timestamp in ms, 0 if unknown
     */
    async getLastActivity(sid) {
        const last = await this.getStateAsync(`${sid}.info.lastActivity`);
        if (last && typeof last.val === 'number' && last.val > 0) {
            return last.val;
        }
        // objects of older versions: time the session went offline
        const online = await this.getStateAsync(`${sid}.info.online`);
        return online && online.val === false ? online.lc || online.ts || 0 : 0;
    }
    /**
     * Forgets sessions whose objects were deleted (e.g. by the user in admin), so they are
     * recreated completely on the next update instead of writing states without objects.
     *
     * @param id full object id
     * @param obj new object, null if deleted
     */
    onObjectChange(id, obj) {
        if (obj || !id.startsWith(`${this.namespace}.`)) {
            return;
        }
        const sid = id.substring(this.namespace.length + 1).split('.')[0];
        if (sid === 'info') {
            return;
        }
        if (this.createdDevices.delete(sid)) {
            this.log.debug(`Objects of device ${sid} were deleted`);
        }
        // a deleted device that is no longer active must not be recreated by markDeviceOffline
        this.activeDevices.delete(sid);
        delete this.laststates[sid];
    }
    /**
     * Handles commands written by the user.
     *
     * @param id full state id
     * @param state new state
     */
    onStateChange(id, state) {
        if (!id || !state || state.ack) {
            return;
        }
        this.handleCommand(id.substring(this.namespace.length + 1), state).catch(e => this.log.warn(`Command ${id} failed: ${e.message}`));
    }
    /**
     * Executes a command for a client.
     *
     * @param id state id without namespace
     * @param state new state
     */
    async handleCommand(id, state) {
        if (id.startsWith('info.') || id.includes('.info.') || id.includes('.media.') || id.includes('.posters.')) {
            // read-only data written by the user: confirm the value as before
            await this.setStateAsync(id, state.val, true);
            return;
        }
        if (id === 'server.refreshLibrary') {
            if (!this.apiKey) {
                this.log.warn('No API key configured, commands cannot be sent.');
                return;
            }
            await this.apiPost('Library/Refresh');
            await this.setStateAsync(id, state.val, true);
            return;
        }
        if (id.startsWith('server.')) {
            await this.setStateAsync(id, state.val, true);
            return;
        }
        const key = id.substring(0, id.indexOf('.'));
        const cmd = id.substring(key.length + 1);
        if (!key || !cmd.startsWith('command.')) {
            this.log.info(`Not supported command: ${id}`);
            return;
        }
        if (!this.apiKey) {
            this.log.warn('No API key configured, commands cannot be sent.');
            return;
        }
        const sessionId = this.sessionOf.get(key);
        if (!sessionId) {
            this.log.warn(`Command ${cmd} not sent: client ${key} is not connected to the server`);
            return;
        }
        const base = `Sessions/${encodeURIComponent(sessionId)}`;
        if (SIMPLE_COMMANDS[cmd]) {
            await this.apiPost(`${base}/${SIMPLE_COMMANDS[cmd]}`);
            await this.setStateAsync(id, state.val, true);
            return;
        }
        const text = state.val === null || state.val === undefined ? '' : String(state.val);
        switch (cmd) {
            case 'command.message':
                await this.apiPost(`${base}/Message`, { Header: 'ioBroker', Text: text, TimeoutMs: 5000 });
                await this.setStateAsync(id, '', true);
                break;
            case 'command.dialog': {
                const sep = text.indexOf('|');
                const body = sep !== -1
                    ? { Header: text.substring(0, sep), Text: text.substring(sep + 1) }
                    : { Header: 'ioBroker', Text: text };
                await this.apiPost(`${base}/Message`, body);
                await this.setStateAsync(id, '', true);
                break;
            }
            case 'command.seek': {
                const percent = Number(state.val);
                const duration = this.durationOf.get(key) || 0;
                if (!Number.isFinite(percent) || percent < 0 || percent > 100) {
                    this.log.warn(`Invalid position "${String(state.val)}", expected 0-100 %`);
                    return;
                }
                if (!duration) {
                    this.log.warn(`Command seek not sent: nothing with a known duration is playing on ${key}`);
                    return;
                }
                const ticks = Math.round((duration * percent) / 100);
                // Emby reads the position from the body, Jellyfin from the query
                await this.apiPost(`${base}/Playing/Seek?SeekPositionTicks=${ticks}`, {
                    Command: 'Seek',
                    SeekPositionTicks: ticks,
                });
                await this.setStateAsync(id, Math.round(percent * 10) / 10, true);
                break;
            }
            case 'command.generalCommand': {
                let command;
                try {
                    command = JSON.parse(text);
                }
                catch {
                    this.log.warn(`Invalid JSON for generalCommand: ${text}`);
                    return;
                }
                if (!command || typeof command.Name !== 'string' || !command.Name) {
                    this.log.warn('generalCommand needs at least {"Name": "..."}');
                    return;
                }
                const args = {};
                if (command.Arguments && typeof command.Arguments === 'object') {
                    for (const [k, v] of Object.entries(command.Arguments)) {
                        args[k] = String(v);
                    }
                }
                await this.apiPost(`${base}/Command`, { Name: command.Name, Arguments: args });
                await this.setStateAsync(id, '', true);
                break;
            }
            case 'command.volume': {
                const volume = Math.round(Number(state.val));
                if (!Number.isFinite(volume) || volume < 0 || volume > 100) {
                    this.log.warn(`Invalid volume "${String(state.val)}", expected 0-100`);
                    return;
                }
                await this.apiPost(`${base}/Command`, { Name: 'SetVolume', Arguments: { Volume: String(volume) } });
                await this.setStateAsync(id, volume, true);
                break;
            }
            default:
                this.log.info(`Not supported command: ${cmd} | ${id}`);
                break;
        }
    }
    /**
     * Converts a plain-text API key stored by older versions into an encrypted value.
     *
     * @returns true if the instance object was rewritten (the instance is restarted then)
     */
    async migratePlainApiKey() {
        // @KI_HINWEIS: Since `apikey` is listed in encryptedNative, js-controller decrypts it on start.
        // Values without the `$/aes-192-cbc:` prefix are treated as legacy (XOR) encrypted, so a key that
        // versions <= 1.3.0 stored in plain text arrives as garbage in this.config. Emby/Jellyfin API keys
        // are 32 hex characters; a raw value of exactly that form is plain text and gets encrypted once.
        // Anything else (aes or real legacy encryption) is left alone and this.config is trusted.
        const obj = await this.getForeignObjectAsync(`system.adapter.${this.namespace}`);
        const raw = obj?.native ? obj.native.apikey : '';
        if (typeof raw !== 'string' || !/^[0-9a-f]{32}$/i.test(raw)) {
            return false;
        }
        this.log.info('Encrypting API key stored in plain text by an older version.');
        this.apiKey = raw;
        // updateConfig() encrypts all encryptedNative attributes itself
        await this.updateConfig({ apikey: raw });
        return true;
    }
    /** Starts the periodic REST poll of the sessions and the device cleanup. */
    startPolling() {
        // undefined for instances created by older versions: use the defaults from io-package
        const seconds = this.config.pollInterval === undefined ? 60 : Number(this.config.pollInterval) || 0;
        if (seconds <= 0 || !this.apiKey) {
            return;
        }
        // @KI_HINWEIS: Emby pushes "Sessions" only on changes (start, stop, pause). The poll keeps
        // info.lastActivity current, catches missed pushes and drives the optional device cleanup.
        this.pollTimer = this.setInterval(() => {
            if (this.connection && this.connection.readyState === WebSocket.OPEN) {
                void this.refreshSessions(true);
            }
        }, Math.max(MIN_POLL_SECONDS, seconds) * 1000);
    }
    /**
     * Device id the adapter uses towards the server. Stable per instance so it does not
     * register as a new device on every start and does not collide with other installations.
     */
    ownDeviceId() {
        return `ioBroker-${this.namespace}`;
    }
    /** Opens the WebSocket connection. Reconnects are scheduled from the close handler only. */
    tryConnect() {
        this.retryTimer = null;
        if (this.stopping) {
            return;
        }
        const prefix = this.config.isSSL ? 'wss://' : 'ws://';
        const url = `${prefix}${this.config.ip}?api_key=${encodeURIComponent(this.apiKey)}&deviceId=${encodeURIComponent(this.ownDeviceId())}`;
        this.log.debug(`try to connect to: ${this.config.ip}`);
        let connection;
        try {
            connection = new WebSocket(url);
        }
        catch (e) {
            this.log.warn(`Connection could not be established: ${e.message}`);
            this.scheduleReconnect();
            return;
        }
        this.connection = connection;
        connection.onopen = () => this.onSocketOpen();
        connection.onmessage = event => this.onSocketMessage(event);
        // WHATWG WebSocket: an error is always followed by a close event, reconnect is handled there
        connection.onerror = event => {
            const message = event.message;
            this.log.debug(`WebSocket error: ${message || 'unknown'}`);
        };
        connection.onclose = event => this.onSocketClose(event);
    }
    /** Schedules exactly one reconnect attempt. */
    scheduleReconnect() {
        if (this.stopping || this.retryTimer) {
            return;
        }
        this.retryTimer = this.setTimeout(() => this.tryConnect(), RECONNECT_DELAY_MS);
    }
    /** Connection established: subscribe to session updates and start keepalive. */
    onSocketOpen() {
        this.log.info(`connected with emby (${this.config.ip})`);
        void this.setState('info.connection', true, true);
        // @KI_HINWEIS: Emby 4.10 pushes "Sessions" only when something changes (the interval in the
        // data is ignored) and does not answer KeepAlive. A "no data" watchdog would therefore reconnect
        // on every quiet server. The periodic KeepAlive keeps traffic on the socket, so a dead TCP
        // connection ends in a close event and the normal reconnect.
        this.connection?.send(JSON.stringify({ MessageType: 'SessionsStart', Data: '0,10000' }));
        this.startKeepAlive(DEFAULT_KEEPALIVE_MS);
        // the server sends the first "Sessions" message only after the next change, so read the
        // current state right away (also brings states up to date after a reconnect)
        void this.refreshSessions();
        void this.refreshServerInfo();
        if (!this.serverInfoTimer && this.apiKey) {
            this.serverInfoTimer = this.setInterval(() => void this.refreshServerInfo(), SERVER_INFO_INTERVAL_MS);
        }
    }
    /**
     * Socket closed (server restart, network loss, idle timeout).
     *
     * @param event close event
     */
    onSocketClose(event) {
        if (this.keepAliveTimer) {
            this.clearInterval(this.keepAliveTimer);
            this.keepAliveTimer = null;
        }
        this.connection = null;
        if (this.stopping) {
            return;
        }
        void this.setState('info.connection', false, true);
        this.log.warn(`Connection to emby closed (code ${event?.code}${event?.reason ? `, ${event.reason}` : ''}). Reconnect in ${RECONNECT_DELAY_MS / 1000}s`);
        this.scheduleReconnect();
    }
    /**
     * Sends KeepAlive messages in the given interval.
     *
     * @param intervalMs interval in milliseconds
     */
    startKeepAlive(intervalMs) {
        if (this.keepAliveTimer) {
            this.clearInterval(this.keepAliveTimer);
        }
        this.keepAliveTimer = this.setInterval(() => {
            if (this.connection && this.connection.readyState === WebSocket.OPEN) {
                this.connection.send(JSON.stringify({ MessageType: 'KeepAlive' }));
            }
        }, intervalMs);
    }
    /**
     * Dispatches incoming WebSocket messages by MessageType.
     *
     * @param e message event
     */
    onSocketMessage(e) {
        let msg;
        try {
            msg = JSON.parse(typeof e.data === 'string' ? e.data : String(e.data));
        }
        catch {
            this.log.debug(`Ignoring non-JSON message: ${String(e.data).substring(0, 200)}`);
            return;
        }
        this.log.silly(JSON.stringify(msg));
        switch (msg?.MessageType) {
            case 'Sessions':
                this.queueSessions(msg.Data);
                break;
            case 'ForceKeepAlive': {
                // Data = timeout in seconds after which the server drops the connection; answer at half of it
                const seconds = Number(msg.Data);
                if (Number.isFinite(seconds) && seconds > 0) {
                    this.startKeepAlive(Math.max(5_000, (seconds * 1000) / 2));
                }
                if (this.connection && this.connection.readyState === WebSocket.OPEN) {
                    this.connection.send(JSON.stringify({ MessageType: 'KeepAlive' }));
                }
                break;
            }
            case 'KeepAlive':
                break;
            case 'ServerRestarting':
            case 'ServerShuttingDown':
                this.log.info(`Server message: ${msg.MessageType}`);
                break;
            default:
                this.log.debug(`Unhandled message type: ${msg?.MessageType}`);
                break;
        }
    }
    /**
     * Returns true if a session should be skipped according to the ignore list (comma separated
     * session ids or device ids, exact match).
     *
     * @param d session from the server
     */
    isIgnored(d) {
        const list = String(this.config.deviceIds || '')
            .split(/[\s,;]+/)
            .map(s => s.trim())
            .filter(Boolean);
        return (list.includes(d.Id) ||
            (!!d.DeviceId && list.includes(d.DeviceId)) ||
            list.includes(this.deviceKey(d.DeviceId, d.Id)));
    }
    /** Reads version, update and restart information of the server (needs the API key). */
    async refreshServerInfo() {
        if (!this.apiKey) {
            return;
        }
        try {
            const res = await this.apiRequest('GET', 'System/Info');
            const info = (await res.json());
            await this.setValue('server.name', info.ServerName || '');
            await this.setValue('server.version', info.Version || '');
            await this.setValue('server.updateAvailable', info.HasUpdateAvailable === true);
            await this.setValue('server.restartPending', info.HasPendingRestart === true);
        }
        catch (e) {
            this.log.debug(`Could not read server info: ${e.message}`);
        }
    }
    /**
     * Handles sendTo commands.
     *
     * `sendTo('emby.0', 'message', { text, header?, timeoutMs?, device? })` shows a message on one client
     * (device key, session id, device id or device name) or, without `device`, on all connected clients
     * that can display messages.
     *
     * @param obj message from sendTo
     */
    onMessage(obj) {
        if (!obj || typeof obj !== 'object' || !obj.command) {
            return;
        }
        const reply = (result) => {
            if (obj.callback) {
                this.sendTo(obj.from, obj.command, result, obj.callback);
            }
        };
        if (obj.command !== 'message' && obj.command !== 'send') {
            reply({ error: `Unknown command ${obj.command}` });
            return;
        }
        this.sendMessageToClients(obj.message)
            .then(sent => reply(sent.length ? { result: 'ok', sent } : { error: 'No matching client connected' }))
            .catch(e => reply({ error: e.message }));
    }
    /**
     * Shows a message on the matching connected clients.
     *
     * @param payload message text or object with text, header, timeoutMs and device
     * @returns device keys the message was sent to
     */
    async sendMessageToClients(payload) {
        const p = typeof payload === 'string' ? { text: payload } : payload || {};
        const text = p.text === undefined || p.text === null ? '' : String(p.text);
        if (!text) {
            throw new Error('Parameter "text" is missing');
        }
        if (!this.apiKey) {
            throw new Error('No API key configured');
        }
        const timeoutMs = Number(p.timeoutMs);
        const body = { Header: p.header ? String(p.header) : 'ioBroker', Text: text };
        if (Number.isFinite(timeoutMs) && timeoutMs > 0) {
            body.TimeoutMs = Math.round(timeoutMs);
        }
        const wanted = p.device === undefined || p.device === null ? '' : String(p.device);
        const sent = [];
        for (const [key, sessionId] of this.sessionOf) {
            const commands = await this.getStateAsync(`${key}.info.supportedCommands`);
            if (!String(commands?.val || '').includes('DisplayMessage')) {
                continue;
            }
            if (wanted) {
                const name = await this.getStateAsync(`${key}.info.deviceName`);
                const deviceId = await this.getStateAsync(`${key}.info.deviceId`);
                if (![key, sessionId, String(deviceId?.val || ''), String(name?.val || '')].includes(wanted)) {
                    continue;
                }
            }
            await this.apiPost(`Sessions/${encodeURIComponent(sessionId)}/Message`, body);
            sent.push(key);
        }
        return sent;
    }
    /**
     * Whether a session is a connection of this adapter (this or another instance) and not a client.
     *
     * @param deviceId Emby device id of the session
     */
    isIoBrokerConnection(deviceId) {
        // 00001 = fixed id used by versions <= 1.3.0, ioBroker-emby.N = id of every instance since 2.0.0
        return deviceId === '00001' || (!!deviceId && deviceId.startsWith(`ioBroker-${this.name}.`));
    }
    /**
     * Removes the objects that versions <= 2.x created for an ioBroker connection, once per session.
     *
     * @param sessionId session id of the ioBroker connection
     */
    async removeIoBrokerDevice(sessionId) {
        if (this.checkedOwnSessions.has(sessionId)) {
            return;
        }
        this.checkedOwnSessions.add(sessionId);
        const sid = sessionId.replace(this.FORBIDDEN_CHARS, '_');
        const obj = await this.getObjectAsync(sid);
        // only objects without stored device id, so a client with this session id is never touched
        if (obj?.type === 'device' && !obj.native?.deviceId) {
            this.log.info(`Removed objects ${sid} that an older version created for an ioBroker connection`);
            await this.deleteDeviceObjects(sid);
        }
    }
    /**
     * Object id of a client. Derived from the Emby device id, which stays the same across reconnects,
     * unlike the session id.
     *
     * @param deviceId Emby device id
     * @param sessionId session id, used only if the server reports no device id
     */
    deviceKey(deviceId, sessionId) {
        // @KI_HINWEIS: Emby device ids are hex strings, UUIDs or (web clients) long base64 strings of the
        // user agent. Short ids with safe characters are used as they are, all others are hashed, so the
        // object id is always short, stable and free of forbidden characters.
        if (!deviceId) {
            return sessionId.replace(this.FORBIDDEN_CHARS, '_');
        }
        if (PLAIN_DEVICE_ID.test(deviceId) && !RESERVED_IDS.has(deviceId)) {
            return deviceId;
        }
        return (0, node_crypto_1.createHash)('sha1').update(deviceId).digest('hex').substring(0, 16);
    }
    /**
     * Writes a state only if its value changed.
     *
     * @param id state id below the namespace
     * @param val value
     */
    async setValue(id, val) {
        await this.setStateChangedAsync(id, { val, ack: true });
    }
    /**
     * Processes a Sessions message: updates all reported sessions and marks sessions that are no
     * longer reported as offline.
     *
     * @param list sessions
     */
    async handleSessions(list) {
        if (!this.initialSessionsLoaded) {
            // devices left over from before an adapter restart are marked offline on the first message
            for (const { sid } of await this.getSessionDevices()) {
                this.activeDevices.add(sid);
            }
            this.initialSessionsLoaded = true;
        }
        // one entry per client; a client can have several sessions (e.g. two browser tabs)
        const byKey = new Map();
        for (const d of list) {
            if (!d || typeof d.Id !== 'string' || d.Id === '') {
                continue;
            }
            if (this.isIoBrokerConnection(d.DeviceId)) {
                await this.removeIoBrokerDevice(d.Id);
                continue;
            }
            if (this.isIgnored(d)) {
                this.log.debug(`Device skipped: ${d.DeviceName}`);
                continue;
            }
            const key = this.deviceKey(d.DeviceId, d.Id);
            const other = byKey.get(key);
            if (!other || this.preferSession(d, other)) {
                byKey.set(key, d);
            }
        }
        for (const [key, d] of byKey) {
            if (key !== d.Id && !this.createdDevices.has(key) && (await this.getObjectAsync(d.Id))) {
                // objects of versions <= 2.x use the session id
                await this.migrateDevice(d.Id, key);
            }
            this.sessionOf.set(key, d.Id);
            // @KI_HINWEIS: a web client reports SupportsRemoteControl=false while its page is in the
            // background or reconnecting, so the objects are rebuilt whenever this signature changes.
            const signature = JSON.stringify([d.DeviceName, !!d.SupportsRemoteControl, d.SupportedCommands || []]);
            if (this.createdDevices.get(key) !== signature) {
                await this.createDeviceObjects(key, d);
                this.createdDevices.set(key, signature);
            }
            await this.updateSession(key, d);
        }
        for (const key of this.activeDevices) {
            if (!byKey.has(key)) {
                this.sessionOf.delete(key);
                await this.markDeviceOffline(key);
            }
        }
        this.activeDevices = new Set(byKey.keys());
    }
    /**
     * Chooses between two sessions of the same client: playing before idle, remote control before
     * none, then the most recent activity.
     *
     * @param a candidate
     * @param b session chosen so far
     * @returns true if a is the better choice
     */
    preferSession(a, b) {
        if (!!a.NowPlayingItem !== !!b.NowPlayingItem) {
            return !!a.NowPlayingItem;
        }
        if (!!a.SupportsRemoteControl !== !!b.SupportsRemoteControl) {
            return !!a.SupportsRemoteControl;
        }
        return (Date.parse(a.LastActivityDate || '') || 0) > (Date.parse(b.LastActivityDate || '') || 0);
    }
    /**
     * Updates the states of one client.
     *
     * @param sid object id of the client (device key)
     * @param d session from the server
     */
    async updateSession(sid, d) {
        await this.setValue(`${sid}.info.online`, true);
        await this.setValue(`${sid}.info.sessionId`, d.Id);
        await this.setValue(`${sid}.info.deviceId`, d.DeviceId || '');
        await this.setValue(`${sid}.info.deviceName`, d.DeviceName || '');
        await this.setValue(`${sid}.info.userName`, d.UserName || '');
        if (Array.isArray(d.SupportedCommands)) {
            await this.setValue(`${sid}.info.supportedCommands`, JSON.stringify(d.SupportedCommands));
        }
        const lastActivity = Date.parse(d.LastActivityDate);
        if (Number.isFinite(lastActivity)) {
            await this.setValue(`${sid}.info.lastActivity`, lastActivity);
        }
        const npi = d.NowPlayingItem;
        const ps = d.PlayState || {};
        if (!npi) {
            this.durationOf.delete(sid);
            await this.clearMedia(sid);
            this.changeState(sid, 'idle');
            return;
        }
        const str = (v) => (v === undefined || v === null ? '' : String(v));
        const num = (v) => (typeof v === 'number' && Number.isFinite(v) ? v : null);
        const list = (v) => (Array.isArray(v) ? v.join(',') : '');
        await this.setValue(`${sid}.media.endtime`, typeof npi.RunTimeTicks === 'number' && typeof ps.PositionTicks === 'number'
            ? formatEndTime((npi.RunTimeTicks - ps.PositionTicks) / 10000)
            : '');
        await this.setValue(`${sid}.media.title`, str(npi.Name));
        await this.setValue(`${sid}.media.description`, str(npi.Overview));
        await this.setValue(`${sid}.media.type`, str(npi.Type));
        await this.setValue(`${sid}.media.rating`, num(npi.CommunityRating));
        await this.setValue(`${sid}.media.year`, num(npi.ProductionYear));
        await this.setValue(`${sid}.media.tags`, list(npi.Taglines));
        await this.setValue(`${sid}.media.genres`, list(npi.Genres));
        await this.setValue(`${sid}.media.backdropimage`, list(npi.BackdropImageTags));
        await this.setValue(`${sid}.media.ratio`, num(npi.PrimaryImageAspectRatio));
        await this.setValue(`${sid}.media.agerating`, str(npi.OfficialRating));
        await this.setValue(`${sid}.media.originaltitle`, str(npi.OriginalTitle));
        await this.setValue(`${sid}.media.seasonName`, str(npi.SeasonName));
        await this.setValue(`${sid}.media.seriesName`, npi.SeasonName !== undefined ? str(npi.SeriesName) : '');
        await this.setValue(`${sid}.media.isMuted`, ps.IsMuted === true);
        if (typeof ps.VolumeLevel === 'number' &&
            d.SupportsRemoteControl &&
            Array.isArray(d.SupportedCommands) &&
            d.SupportedCommands.includes('SetVolume')) {
            await this.setValue(`${sid}.command.volume`, ps.VolumeLevel);
        }
        const duration = typeof npi.RunTimeTicks === 'number' && npi.RunTimeTicks > 0 ? npi.RunTimeTicks : 0;
        const position = typeof ps.PositionTicks === 'number' && ps.PositionTicks >= 0 ? ps.PositionTicks : null;
        this.durationOf.set(sid, duration);
        await this.setValue(`${sid}.media.duration`, duration ? Math.round(duration / TICKS_PER_SECOND) : null);
        await this.setValue(`${sid}.media.elapsed`, position !== null ? Math.round(position / TICKS_PER_SECOND) : null);
        if (d.SupportsRemoteControl) {
            await this.setValue(`${sid}.command.seek`, duration && position !== null ? Math.round((position / duration) * 1000) / 10 : null);
        }
        const basePoster = `${this.baseUrl()}/Items/`;
        let posters = ['', '', ''];
        if (npi.Type === 'Episode') {
            posters = [
                `${basePoster}${npi.SeriesId}/Images/Primary`,
                `${basePoster}${npi.SeasonId}/Images/Primary`,
                `${basePoster}${npi.Id}/Images/Primary`,
            ];
        }
        else if (npi.Type === 'Movie') {
            posters = [`${basePoster}${npi.Id}/Images/Primary`, '', ''];
        }
        await this.setValue(`${sid}.posters.main`, posters[0]);
        await this.setValue(`${sid}.posters.season`, posters[1]);
        await this.setValue(`${sid}.posters.episode`, posters[2]);
        if (ps.MediaSourceId !== undefined && !ps.IsPaused) {
            this.changeState(sid, 'playing');
        }
        else {
            this.changeState(sid, 'paused');
        }
    }
    /**
     * Resets all media states of a session.
     *
     * @param sid session id
     */
    async clearMedia(sid) {
        for (const s of [
            'title',
            'description',
            'seasonName',
            'seriesName',
            'endtime',
            'tags',
            'genres',
            'backdropimage',
            'agerating',
            'originaltitle',
        ]) {
            await this.setValue(`${sid}.media.${s}`, '');
        }
        for (const s of ['rating', 'year', 'ratio']) {
            await this.setValue(`${sid}.media.${s}`, null);
        }
        // clients last seen by an older version (marked offline at start) have no elapsed/duration yet
        if (await this.getObjectAsync(`${sid}.media.elapsed`)) {
            await this.setValue(`${sid}.media.elapsed`, null);
            await this.setValue(`${sid}.media.duration`, null);
        }
        // only clients with remote control have command.seek
        if (await this.getObjectAsync(`${sid}.command.seek`)) {
            await this.setValue(`${sid}.command.seek`, null);
        }
        await this.setValue(`${sid}.media.type`, 'None');
        await this.setValue(`${sid}.media.isMuted`, false);
        for (const s of ['main', 'season', 'episode']) {
            await this.setValue(`${sid}.posters.${s}`, '');
        }
    }
    /**
     * Moves the objects of version 2.x (device object stored the device id in native.deviceId, but the
     * object id was the session id) to the device key, once on start.
     */
    async migrateStoredDevices() {
        for (const { sid, obj } of await this.getSessionDevices()) {
            const deviceId = obj?.native?.deviceId || '';
            if (!deviceId) {
                // versions <= 1.3.0: migrated as soon as the server reports the session again
                continue;
            }
            const key = this.deviceKey(deviceId, sid);
            if (key === sid) {
                continue;
            }
            if (await this.getObjectAsync(key)) {
                // an older session of a client that was already migrated
                this.log.info(`Removing objects of old session ${sid} of client "${nameOf(obj.common.name)}"`);
                await this.delObjectAsync(sid, { recursive: true });
                continue;
            }
            await this.migrateDevice(sid, key);
        }
    }
    /**
     * Moves all objects and states of a client to a new object id: objects including common.custom
     * (history settings), current values and the membership in rooms and functions. The old objects are
     * deleted afterwards.
     *
     * @param oldId old object id (session id)
     * @param newId new object id (device key)
     */
    async migrateDevice(oldId, newId) {
        const oldPrefix = `${this.namespace}.${oldId}`;
        const newPrefix = `${this.namespace}.${newId}`;
        const objects = await this.getForeignObjectsAsync(`${oldPrefix}.*`);
        const device = await this.getForeignObjectAsync(oldPrefix);
        if (device) {
            objects[oldPrefix] = device;
        }
        const ids = Object.keys(objects)
            .filter(id => id === oldPrefix || id.startsWith(`${oldPrefix}.`))
            .sort((a, b) => a.length - b.length);
        if (!ids.length) {
            return;
        }
        // @KI_HINWEIS: ioBroker has no rename. Objects are copied with common.custom, so history adapters
        // continue on the new id; the data already recorded stays under the old id in the database.
        for (const id of ids) {
            const obj = objects[id];
            const target = newPrefix + id.substring(oldPrefix.length);
            const common = { ...obj.common };
            if (obj.type === 'state' && !STATE_TYPES.has(String(common.type))) {
                // corrected by createDeviceObjects afterwards, set here to avoid warnings of the js-controller
                common.type = common.role === 'button' ? 'boolean' : 'mixed';
            }
            await this.setForeignObjectAsync(target, {
                type: obj.type,
                common,
                native: obj.native || {},
            });
            if (obj.type === 'state') {
                const st = await this.getForeignStateAsync(id);
                if (st) {
                    await this.setForeignStateAsync(target, { val: st.val, ack: true, ts: st.ts, q: st.q });
                }
            }
        }
        const enums = await this.getForeignObjectsAsync('enum.*', 'enum');
        for (const [enumId, enumObj] of Object.entries(enums)) {
            const members = Array.isArray(enumObj?.common?.members) ? enumObj.common.members : [];
            if (!members.some(m => ids.includes(m))) {
                continue;
            }
            const updated = members.map(m => (ids.includes(m) ? newPrefix + m.substring(oldPrefix.length) : m));
            await this.extendForeignObjectAsync(enumId, { common: { members: updated } });
        }
        this.log.info(`Migrated objects of "${nameOf(device?.common?.name ?? oldId)}" from ${oldId} to ${newId}. Scripts and visualisations using the old id have to be adjusted.`);
        await this.deleteDeviceObjects(oldId);
    }
    /**
     * Deletes all objects of a client and forgets its runtime state.
     *
     * @param sid object id of the client
     */
    async deleteDeviceObjects(sid) {
        this.activeDevices.delete(sid);
        this.createdDevices.delete(sid);
        this.sessionOf.delete(sid);
        if (this.timeoutplays[sid]) {
            this.clearTimeout(this.timeoutplays[sid]);
        }
        delete this.timeoutplays[sid];
        delete this.timeoutstates[sid];
        delete this.timeoutstarted[sid];
        delete this.laststates[sid];
        this.durationOf.delete(sid);
        await this.delObjectAsync(sid, { recursive: true });
    }
    /**
     * Marks a client the server no longer reports (client closed, TV switched off).
     *
     * @param sid object id of the client
     */
    async markDeviceOffline(sid) {
        if (!(await this.getObjectAsync(sid))) {
            // objects were deleted in the meantime, nothing to update
            return;
        }
        this.log.debug(`Client ${sid} is no longer active`);
        // objects created by versions <= 1.3.0 have no info.online yet
        await this.extendObjectAsync(`${sid}.info.online`, roState('Session is active on the server', 'boolean', 'indicator.reachable'));
        await this.setValue(`${sid}.info.online`, false);
        await this.clearMedia(sid);
        this.changeState(sid, 'idle');
    }
    /**
     * Sets media.state; changes away from "playing" are delayed by the configured timeout
     * to suppress short flickering.
     *
     * @param id session id
     * @param state idle | paused | playing
     */
    changeState(id, state) {
        if (this.laststates[id] === state) {
            return;
        }
        if (state === 'playing') {
            if (this.timeoutplays[id]) {
                this.clearTimeout(this.timeoutplays[id]);
                this.timeoutplays[id] = null;
            }
            this.timeoutstarted[id] = false;
            void this.setState(`${id}.media.state`, state, true);
        }
        else {
            this.timeoutstates[id] = state;
            if (!this.timeoutstarted[id]) {
                this.timeoutstarted[id] = true;
                this.timeoutplays[id] = this.setTimeout(() => {
                    this.timeoutplays[id] = null;
                    this.timeoutstarted[id] = false;
                    void this.setState(`${id}.media.state`, this.timeoutstates[id], true);
                }, Number(this.config.timeout) || 0);
            }
        }
        this.laststates[id] = state;
    }
    /**
     * Creates or updates the objects of a client. Uses extendObject so objects created by older
     * versions with invalid types (e.g. common.type "button") are corrected.
     *
     * @param sid object id of the client (device key)
     * @param device session from the server
     */
    async createDeviceObjects(sid, device) {
        await this.setObjectNotExistsAsync(sid, {
            type: 'device',
            common: { name: device.DeviceName || sid },
            native: {},
        });
        // the display name can be changed in the Emby dashboard (Devices), keep it in sync
        await this.extendObjectAsync(sid, {
            common: { name: device.DeviceName || sid },
            native: { deviceId: device.DeviceId || '' },
        });
        const objs = {
            info: { type: 'channel', common: { name: 'Info' }, native: {} },
            media: { type: 'channel', common: { name: 'Media Info' }, native: {} },
            posters: { type: 'channel', common: { name: 'Media Posters' }, native: {} },
            'info.online': roState('Session is active on the server', 'boolean', 'indicator.reachable'),
            'info.deviceId': roState('Emby device id', 'string', 'text'),
            'info.sessionId': roState('Current Emby session id', 'string', 'text'),
            'info.deviceName': roState('Name of the Device now playing', 'string', 'info.name'),
            'info.userName': roState('Logged in User', 'string', 'info.name'),
            'info.supportedCommands': roState('List of supported Commands', 'string', 'json'),
            'info.lastActivity': roState('Last activity of the client', 'number', 'date'),
            'media.state': {
                type: 'state',
                common: {
                    name: 'State of the Client',
                    role: 'media.state',
                    type: 'string',
                    read: true,
                    write: false,
                    states: { idle: 'Idle', paused: 'Paused', playing: 'Playing' },
                },
                native: {},
            },
            'media.isMuted': roState('If Player is muted', 'boolean', 'media.mute'),
            'media.title': roState('Title of file playing', 'string', 'media.title'),
            'media.seriesName': roState('Name of serie now playing', 'string', 'media.title'),
            'media.seasonName': roState('Name of season now playing', 'string', 'media.season'),
            'media.type': roState('Type of file playing', 'string'),
            'media.description': roState('Description of file playing', 'string'),
            'media.endtime': roState('Endtime of playing video/audio', 'string'),
            'media.rating': roState('Community Rating', 'number'),
            'media.year': roState('Production Year', 'number'),
            'media.tags': roState('Tags', 'string'),
            'media.genres': roState('Genres', 'string'),
            'media.backdropimage': roState('BackdropImageTags', 'string'),
            'media.ratio': roState('Ratio', 'number'),
            'media.agerating': roState('Age Rating', 'string'),
            'media.originaltitle': roState('Original Title', 'string'),
            'media.elapsed': {
                type: 'state',
                common: {
                    name: 'Playback position',
                    role: 'media.elapsed',
                    type: 'number',
                    unit: 's',
                    read: true,
                    write: false,
                },
                native: {},
            },
            'media.duration': {
                type: 'state',
                common: {
                    name: 'Duration',
                    role: 'media.duration',
                    type: 'number',
                    unit: 's',
                    read: true,
                    write: false,
                },
                native: {},
            },
            'posters.main': roState('Main Poster', 'string', 'url'),
            'posters.episode': roState('Episode Poster', 'string', 'url'),
            'posters.season': roState('Season Poster', 'string', 'url'),
        };
        if (device.SupportsRemoteControl) {
            objs.command = { type: 'channel', common: { name: 'Control Client' }, native: {} };
            objs['command.pause'] = button('Pause');
            objs['command.play'] = button('Play');
            objs['command.toggleplay'] = button('Toggle Play');
            objs['command.stop'] = button('Stop', 'button.stop');
            objs['command.next'] = button('Next', 'button.next');
            objs['command.previous'] = button('Previous', 'button.prev');
            objs['command.seek'] = {
                type: 'state',
                common: {
                    name: 'Playback position (set to seek)',
                    role: 'media.seek',
                    type: 'number',
                    unit: '%',
                    min: 0,
                    max: 100,
                    read: true,
                    write: true,
                },
                native: {},
            };
            objs['command.generalCommand'] = {
                type: 'state',
                common: {
                    name: 'Send a general command (JSON: {"Name": "...", "Arguments": {...}})',
                    role: 'json',
                    type: 'string',
                    read: false,
                    write: true,
                },
                native: {},
            };
            const supported = Array.isArray(device.SupportedCommands) ? device.SupportedCommands : [];
            for (const c of supported) {
                switch (c) {
                    case 'DisplayMessage':
                        objs['command.message'] = {
                            type: 'state',
                            common: { name: 'Message', role: 'text', type: 'string', read: false, write: true },
                            native: {},
                        };
                        objs['command.dialog'] = {
                            type: 'state',
                            common: { name: 'Message Dialog', role: 'text', type: 'string', read: false, write: true },
                            native: {},
                        };
                        break;
                    case 'GoHome':
                        objs['command.goHome'] = button('GoHome');
                        break;
                    case 'SetVolume':
                        objs['command.volume'] = {
                            type: 'state',
                            common: {
                                name: 'Set Volume (causes crash on some devices)',
                                role: 'level.volume',
                                type: 'number',
                                min: 0,
                                max: 100,
                                read: true,
                                write: true,
                            },
                            native: {},
                        };
                        break;
                    case 'GoToSearch':
                        objs['command.goToSearch'] = button('Go to search');
                        break;
                    case 'Back':
                        objs['command.back'] = button('Back');
                        break;
                    case 'Mute':
                        objs['command.mute'] = button('Mute');
                        break;
                    case 'Unmute':
                    case 'UnMute':
                        objs['command.unmute'] = button('Unmute');
                        break;
                    case 'ToggleMute':
                        objs['command.togglemute'] = button('Toggle Mute');
                        break;
                }
            }
        }
        for (const [id, obj] of Object.entries(objs)) {
            await this.extendObjectAsync(`${sid}.${id}`, obj);
        }
    }
}
if (require.main !== module) {
    // Export the constructor in compact mode
    module.exports = (options) => new EmbyAdapter(options);
}
else {
    // otherwise start the instance directly
    (() => new EmbyAdapter())();
}
//# sourceMappingURL=main.js.map