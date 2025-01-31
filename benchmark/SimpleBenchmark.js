'use strict';

class Benchmark {
    constructor(config) {
        this._nextOp = config.nextOp;
        this.totalOpsCount = config.totalOpsCount;
        this.batchSize = config.batchSize;
        this.opsCount = 0;
    }
    // increments ops counter, starts a new op and returns its promise
    nextOp() {
        this.opsCount++;
        return this._nextOp();
    }
    // chains next op once one of ops finishes to keep constant concurrency of ops
    chainNext(op) {
        return op.then(() => {
            if (this.opsCount < this.totalOpsCount) {
                return this.chainNext(this.nextOp());
            }
        });
    }
    run() {
        // initial batch of ops (no-op promises)
        const batch = new Array(this.batchSize).fill(Promise.resolve());
        const start = new Date();
        return Promise.all(batch.map(this.chainNext.bind(this)))
            .then(() => {
                const finish = new Date();
                const tookSec = (finish - start) / 1000;
                console.log(`Took ${tookSec} seconds for ${this.opsCount} requests`);
                console.log(`Ops/s: ${this.opsCount / tookSec}`);
            });
    }
};

module.exports = Benchmark;
