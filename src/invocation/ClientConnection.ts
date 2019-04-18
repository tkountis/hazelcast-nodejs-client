/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as Promise from 'bluebird';
import * as net from 'net';
import {BitsUtil} from '../BitsUtil';
import {BuildInfo} from '../BuildInfo';
import HazelcastClient from '../HazelcastClient';
import {IOError} from '../HazelcastError';
import Address = require('../Address');
import {DeferredPromise} from '../Util';
import Socket = NodeJS.Socket;
import ClientMessage = require('../ClientMessage');

class WriteQueue {

    private socket: Socket;
    private queue: any = [];
    private error: any;
    private isRunning: boolean;
    private coalescingThreshold: Number = 16384;

    constructor(socket: Socket) {
        this.socket = socket;

    }

    push(request: Buffer, callback: Function): void {
        if (this.error) {
            // There was a write error, there is no point in further trying to write to the socket.
            return process.nextTick(() => {
                callback(this.error);
            });
        }
        this.queue.push({
            request,
            callback,
        });
        this.run();
    }

    run(): void {
        if (!this.isRunning) {
            this.process();
        }
    }

    process(): void {
        const self = this;
        this.whilst(
            () => {
                return self.queue.length > 0;
            },
            (next: any) => {
                self.isRunning = true;
                const buffers = [];
                const callbacks = [];
                let totalLength = 0;
                while (totalLength < self.coalescingThreshold && self.queue.length > 0) {
                    const writeItem = self.queue.shift();
                    try {
                        const data = writeItem.request;
                        totalLength += data.length;
                        buffers.push(data);
                        callbacks.push(writeItem.callback);
                    } catch (err) {
                        writeItem.callback(err);
                        // break and flush what we have
                        break;
                    }
                }
                if (buffers.length === 0) {
                    // No need to invoke socket.write()
                    return next();
                }
                // Before invoking socket.write(), mark that the request has been written to avoid race conditions.
                for (let i = 0; i < callbacks.length; i++) {
                    callbacks[i]();
                }
                self.socket.write(Buffer.concat(buffers, totalLength), (err: Error) => {
                    if (err) {
                        self.setWriteError(err);
                    }
                    // Allow IO between writes
                    setImmediate(next);
                });
            },
            () => {
                // The queue is now empty
                self.isRunning = false;
            },
        );
    }

    private whilst(condition: Function, fn: Function, cb: Function): void {
        let sync = 0;
        const next = (err: any) => {
            if (err) {
                return cb(err);
            }
            if (!condition()) {
                return cb();
            }
            if (sync === 0) {
                sync = 1;
                fn((e: any) => {
                    if (sync === 1) {
                        // sync function
                        sync = 4;
                    }
                    next(e);
                });
                if (sync === 1) {
                    // async function
                    sync = 2;
                }
                return;
            }
            if (sync === 4) {
                // Prevent "Maximum call stack size exceeded"
                return process.nextTick(() => {
                    fn(next);
                });
            }
            // do a sync call as the callback is going to call on a future tick
            fn(next);
        };

        next(undefined);
    }

    private setWriteError(err: any): void {
        err.isSocketError = true;
        this.error = new Error('Socket was closed');
        this.error.isSocketError = true;
        // Use an special flag for items that haven't been written
        this.error.requestNotWritten = true;
        this.error.innerError = err;
        const q = this.queue;
        // Not more items can be added to the queue.
        this.queue = [];
        for (let i = 0; i < q.length; i++) {
            const item = q[i];
            // Use the error marking that it was not written
            item.callback(this.error);
        }
    }
}

export class ClientConnection {
    private address: Address;
    private readonly localAddress: Address;
    private lastReadTimeMillis: number;
    private lastWriteTimeMillis: number;
    private heartbeating = true;
    private client: HazelcastClient;
    private readBuffer: Buffer;
    private readonly startTime: number = Date.now();
    private closedTime: number;
    private connectedServerVersionString: string;
    private connectedServerVersion: number;
    private authenticatedAsOwner: boolean;
    private socket: net.Socket;
    private writeQueue: WriteQueue;

    constructor(client: HazelcastClient, address: Address, socket: net.Socket) {
        this.client = client;
        this.socket = socket;
        this.writeQueue = new WriteQueue(socket);
        this.address = address;
        this.localAddress = new Address(socket.localAddress, socket.localPort);
        this.readBuffer = new Buffer(0);
        this.lastReadTimeMillis = 0;
        this.closedTime = 0;
        this.connectedServerVersionString = null;
        this.connectedServerVersion = BuildInfo.UNKNOWN_VERSION_ID;
    }

    /**
     * Returns the address of local port that is associated with this connection.
     * @returns
     */
    getLocalAddress(): Address {
        return this.localAddress;
    }

    /**
     * Returns the address of remote node that is associated with this connection.
     * @returns
     */
    getAddress(): Address {
        return this.address;
    }

    setAddress(address: Address): void {
        this.address = address;
    }

    write(buffer: Buffer): Promise<void> {
        const deferred = DeferredPromise<void>();
        this.writeQueue.push(buffer, () => { deferred.resolve(); });
        // try {
        //     this.socket.write(buffer, (err: any) => {
        //         if (err) {
        //             deferred.reject(new IOError(err));
        //         } else {
        //             this.lastWriteTimeMillis = Date.now();
        //             deferred.resolve();
        //         }
        //     });
        // } catch (err) {
        //     deferred.reject(new IOError(err));
        // }
        return deferred.promise;
    }

    setConnectedServerVersion(versionString: string): void {
        this.connectedServerVersionString = versionString;
        this.connectedServerVersion = BuildInfo.calculateServerVersionFromString(versionString);
    }

    getConnectedServerVersion(): number {
        return this.connectedServerVersion;
    }

    /**
     * Closes this connection.
     */
    close(): void {
        this.socket.end();
        this.closedTime = Date.now();
    }

    isAlive(): boolean {
        return this.closedTime === 0;
    }

    isHeartbeating(): boolean {
        return this.heartbeating;
    }

    setHeartbeating(heartbeating: boolean): void {
        this.heartbeating = heartbeating;
    }

    isAuthenticatedAsOwner(): boolean {
        return this.authenticatedAsOwner;
    }

    setAuthenticatedAsOwner(asOwner: boolean): void {
        this.authenticatedAsOwner = asOwner;
    }

    getStartTime(): number {
        return this.startTime;
    }

    getLastReadTimeMillis(): number {
        return this.lastReadTimeMillis;
    }

    getLastWriteTimeMillis(): number {
        return this.lastWriteTimeMillis;
    }

    toString(): string {
        return this.address.toString();
    }

    /**
     * Registers a function to pass received data on 'data' events on this connection.
     * @param callback
     */
    registerResponseCallback(callback: Function): void {
        this.socket.on('data', (buffer: Buffer) => {
            this.lastReadTimeMillis = new Date().getTime();
            this.readBuffer = Buffer.concat([this.readBuffer, buffer], this.readBuffer.length + buffer.length);
            while (this.readBuffer.length >= BitsUtil.INT_SIZE_IN_BYTES) {
                const frameSize = this.readBuffer.readInt32LE(0);
                if (frameSize > this.readBuffer.length) {
                    return;
                }
                const message: Buffer = new Buffer(frameSize);
                this.readBuffer.copy(message, 0, 0, frameSize);
                this.readBuffer = this.readBuffer.slice(frameSize);
                callback(message);
            }
        });
        this.socket.on('error', (e: any) => {
            if (e.code === 'EPIPE' || e.code === 'ECONNRESET') {
                this.client.getConnectionManager().destroyConnection(this.address);
            }
        });
    }
}
