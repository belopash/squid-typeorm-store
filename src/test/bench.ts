import {performance} from 'perf_hooks'
import {Store} from '../store'
import {StateManager} from '../utils/stateManager'
import {Item, Order} from './lib/model'
import {databaseDelete, databaseInit, getEntityManager} from './util'

interface BenchResult {
    name: string
    ops: number
    totalMs: number
    opsPerSec: number
    msPerOp: number
}

async function bench(name: string, ops: number, fn: () => Promise<void>): Promise<BenchResult> {
    await fn()

    const start = performance.now()
    await fn()
    const totalMs = performance.now() - start

    return {
        name,
        ops,
        totalMs: Math.round(totalMs * 100) / 100,
        opsPerSec: Math.round(ops / (totalMs / 1000)),
        msPerOp: Math.round((totalMs / ops) * 1000) / 1000,
    }
}

function report(results: BenchResult[]) {
    const nameWidth = Math.max(...results.map((r) => r.name.length), 4)
    const divider = '-'.repeat(nameWidth + 60)

    console.log()
    console.log(divider)
    console.log(
        'name'.padEnd(nameWidth),
        'ops'.padStart(10),
        'total ms'.padStart(12),
        'ms/op'.padStart(10),
        'ops/s'.padStart(12)
    )
    console.log(divider)

    for (const r of results) {
        console.log(
            r.name.padEnd(nameWidth),
            String(r.ops).padStart(10),
            r.totalMs.toFixed(2).padStart(12),
            r.msPerOp.toFixed(3).padStart(10),
            r.opsPerSec.toLocaleString().padStart(12)
        )
    }
    console.log(divider)
    console.log()
}

async function freshStore(): Promise<Store> {
    const em = await getEntityManager()
    return new Store({
        em,
        state: new StateManager({connection: em.connection}),
        postponeWriteOperations: true,
        cacheEntities: true,
    })
}

function makeItems(n: number): Item[] {
    const items: Item[] = []
    for (let i = 0; i < n; i++) {
        items.push(new Item(`item-${i}`, `name-${i}`))
    }
    return items
}

async function benchInsert(n: number): Promise<BenchResult> {
    return bench(`insert (${n})`, n, async () => {
        await databaseDelete()
        await databaseInit([`CREATE TABLE item (id text primary key, name text)`])
        const store = await freshStore()
        await store.track(makeItems(n))
        await store.sync()
    })
}

async function benchUpsert(n: number): Promise<BenchResult> {
    return bench(`save/upsert (${n})`, n, async () => {
        await databaseDelete()
        await databaseInit([`CREATE TABLE item (id text primary key, name text)`])
        const store = await freshStore()
        await store.track(makeItems(n), {replace: true})
        await store.sync()
    })
}

async function benchGetCacheHit(n: number): Promise<BenchResult> {
    await databaseDelete()
    await databaseInit([`CREATE TABLE item (id text primary key, name text)`])
    const store = await freshStore()
    await store.track(makeItems(n))
    await store.sync()

    return bench(`get cache-hit (${n})`, n, async () => {
        for (let i = 0; i < n; i++) {
            await store.get(Item, `item-${i}`)
        }
    })
}

async function benchFindAndAutoUpsertDirty(n: number): Promise<BenchResult> {
    return bench(`find+mutate+save (${n})`, n, async () => {
        await databaseDelete()
        await databaseInit([`CREATE TABLE item (id text primary key, name text)`])
        const store = await freshStore()
        await store.track(makeItems(n))
        await store.sync()

        const items = await store.find(Item, {where: {}})
        for (const item of items) {
            item.name = item.name + '-updated'
        }
        // auto-upsert: just trigger a sync (any write or explicit sync)
        await store.sync()
    })
}

async function benchFindAndSyncClean(n: number): Promise<BenchResult> {
    return bench(`find+save clean (${n})`, n, async () => {
        await databaseDelete()
        await databaseInit([`CREATE TABLE item (id text primary key, name text)`])
        const store = await freshStore()
        await store.track(makeItems(n))
        await store.sync()

        await store.find(Item, {where: {}})
        // auto-upsert: sync evaluates touched rows — all clean, nothing written
        await store.sync()
    })
}

async function benchInsertWithRelation(n: number): Promise<BenchResult> {
    return bench(`insert w/ relation (${n})`, n, async () => {
        await databaseDelete()
        await databaseInit([
            `CREATE TABLE item (id text primary key, name text)`,
            `CREATE TABLE "order" (id text primary key, item_id text REFERENCES item, qty int4)`,
        ])
        const store = await freshStore()
        const item = new Item('root', 'root-item')
        await store.track(item)

        const orders: Order[] = []
        for (let i = 0; i < n; i++) {
            orders.push(new Order({id: `order-${i}`, qty: i, item} as Order))
        }
        await store.track(orders)
        await store.sync()
    })
}

async function benchFullCycle(n: number): Promise<BenchResult> {
    return bench(`full cycle (${n})`, n, async () => {
        await databaseDelete()
        await databaseInit([`CREATE TABLE item (id text primary key, name text)`])
        const store = await freshStore()

        await store.track(makeItems(n))
        await store.sync()

        for (let i = 0; i < n; i++) {
            await store.get(Item, `item-${i}`)
        }

        for (let i = 0; i < n; i += 2) {
            const e = await store.get(Item, `item-${i}`)
            if (e) e.name = 'mutated'
        }

        // auto-upsert handles the dirty rows on next sync
        await store.sync()
    })
}

async function main() {
    const sizes = [1_000, 10_000, 50_000]
    const results: BenchResult[] = []

    for (const n of sizes) {
        results.push(await benchInsert(n))
    }

    for (const n of sizes) {
        results.push(await benchUpsert(n))
    }

    results.push(await benchGetCacheHit(10_000))

    for (const n of [1_000, 10_000]) {
        results.push(await benchFindAndAutoUpsertDirty(n))
    }

    for (const n of [1_000, 10_000]) {
        results.push(await benchFindAndSyncClean(n))
    }

    results.push(await benchInsertWithRelation(5_000))

    for (const n of [1_000, 10_000]) {
        results.push(await benchFullCycle(n))
    }

    report(results)
    process.exit(0)
}

main().catch((err) => {
    console.error(err)
    process.exit(1)
})
