import {createFuture, type Future} from '@subsquid/util-internal'

export class Mutex {
    private waitQueue: Array<Future<void> | undefined> = []
    private head = 0

    async acquire(): Promise<void> {
        this.waitQueue.push(createFuture())
        const idx = this.waitQueue.length - 1
        return idx > this.head ? this.waitQueue[idx - 1]?.promise() : undefined
    }

    release(): void {
        if (this.head >= this.waitQueue.length) return
        const future = this.waitQueue[this.head]
        this.waitQueue[this.head] = undefined
        this.head++
        if (this.head > 64 && this.head > this.waitQueue.length / 2) {
            this.waitQueue = this.waitQueue.slice(this.head)
            this.head = 0
        }
        future?.resolve()
    }

    async wait(): Promise<void> {
        if (this.head >= this.waitQueue.length) return
        return this.waitQueue[this.head]?.promise()
    }
}
