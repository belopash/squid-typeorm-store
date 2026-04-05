import {assertNotNull} from '@subsquid/util-internal'
import expect from 'expect'
import {Equal} from 'typeorm'
import {Store} from '../store'
import {Item, Order} from './lib/model'
import {getEntityManager, useDatabase} from './util'
import {StateManager} from '../utils/stateManager'

describe('Store', function () {
    describe('.track() (INSERT)', function () {
        useDatabase([
            `CREATE TABLE item (id text primary key , name text)`,
            `CREATE TABLE "order" (id text primary key, item_id text REFERENCES item, qty int4)`,
        ])

        it('get single entity', async function () {
            let store = await createStore()
            await store.track(new Item('1', 'a'))
            await expect(store.get(Item, '1')).resolves.toEqual({id: '1', name: 'a'})
        })

        it('get returns same instance as cache', async function () {
            let store = await createStore()
            await store.track(new Item('1', 'a'))
            const a = await store.get(Item, '1')
            const b = await store.get(Item, '1')
            expect(a).toBe(b)
        })

        it('get single entity with relation', async function () {
            let store = await createStore()
            const item = new Item('1', 'a')
            await store.track(item)
            await store.track(new Order({id: '1', qty: 1, item}))
            await expect(store.get(Order, {id: '1', relations: {item: true}})).resolves.toEqual({
                id: '1',
                qty: 1,
                item: {id: '1', name: 'a'},
            })
        })

        it('track single entity', async function () {
            let store = await createStore()
            await store.track(new Item('1', 'a'))
            await expect(getItems(store)).resolves.toEqual([{id: '1', name: 'a'}])
        })

        it('track multiple entities', async function () {
            let store = await createStore()
            await store.track([new Item('1', 'a'), new Item('2', 'b')])
            await expect(getItems(store)).resolves.toEqual([
                {id: '1', name: 'a'},
                {id: '2', name: 'b'},
            ])
        })

        it('track a large amount of entities', async function () {
            let store = await createStore()
            let items: Item[] = []
            for (let i = 0; i < 20000; i++) {
                items.push(new Item('' + i))
            }
            await store.track(items)
            expect(await store.count(Item)).toEqual(items.length)
        })
    })

    describe('Auto upsert (touched + dirty)', function () {
        useDatabase([
            `CREATE TABLE item (id text primary key , name text)`,
            `CREATE TABLE "order" (id text primary key, item_id text REFERENCES item, qty int4)`,
        ])

        it('updates loaded row via mutation without track({ replace: true })', async function () {
            let store = await createStore()
            await store.track(new Item('1', 'a'))
            const row1 = assertNotNull(await store.get(Item, '1'))
            row1.name = 'foo'
            await store.track(new Item('2', 'b'))
            await expect(getItems(store)).resolves.toEqual([
                {id: '1', name: 'foo'},
                {id: '2', name: 'b'},
            ])
        })

        it('does not call upsert for touched rows that are unchanged', async function () {
            let store = await createStore()
            await store.track(new Item('1', 'a'))
            await store.get(Item, '1')

            let upsertCalls = 0
            const em = (store as any).em
            const origUpsert = em.upsert.bind(em)
            em.upsert = async (...args: any[]) => {
                upsertCalls++
                return origUpsert(...args)
            }

            await store.sync()
            expect(upsertCalls).toEqual(0)

            em.upsert = origUpsert
        })
    })

    describe('get cached null (non-existent entity)', function () {
        useDatabase([
            `CREATE TABLE item (id text primary key, name text)`,
        ])

        it('does not throw when getting a previously looked-up missing entity', async function () {
            let store = await createStore()
            // First call: DB miss — caches the ID as null sentinel
            await expect(store.get(Item, '999')).resolves.toBeUndefined()
            // Second call: state.get() returns null — must not throw
            await expect(store.get(Item, '999')).resolves.toBeUndefined()
        })
    })

    describe('.track({ replace: true })', function () {
        useDatabase([
            `CREATE TABLE item (id text primary key , name text)`,
            `CREATE TABLE "order" (id text primary key, item_id text REFERENCES item, qty int4)`,
        ])

        it('replaces a different cached instance without throwing', async function () {
            let store = await createStore()
            await store.track(new Item('1', 'a'))
            await expect(store.track(new Item('1', 'x'), {replace: true})).resolves.toBeUndefined()
        })

        it('get() returns the replacement instance after overwrite', async function () {
            let store = await createStore()
            const original = new Item('1', 'a')
            await store.track(original)
            const replacement = new Item('1', 'x')
            await store.track(replacement, {replace: true})
            const cached = await store.get(Item, '1')
            expect(cached).toBe(replacement)
        })

        it('writes the replacement value to the database (upsert)', async function () {
            let store = await createStore()
            await store.track(new Item('1', 'a'))
            await store.track(new Item('1', 'x'), {replace: true})
            await expect(getItems(store)).resolves.toEqual([{id: '1', name: 'x'}])
        })

        it('works for a brand-new id (no prior cached instance)', async function () {
            let store = await createStore()
            await store.track(new Item('1', 'a'), {replace: true})
            await expect(getItems(store)).resolves.toEqual([{id: '1', name: 'a'}])
        })

        it('plain track() still throws on a duplicate id', async function () {
            let store = await createStore()
            await store.track(new Item('1', 'a'))
            await expect(store.track(new Item('1', 'x'))).rejects.toThrow(/already marked as/)
        })

        it('replaces after a delete→upsert transition', async function () {
            let store = await createStore()
            await store.track(new Item('1', 'a'))
            await store.delete(Item, '1')
            await store.track(new Item('1', 'z'), {replace: true})
            await expect(getItems(store)).resolves.toEqual([{id: '1', name: 'z'}])
        })
    })

    describe('.remove()', function () {
        useDatabase([
            `CREATE TABLE item (id text primary key , name text)`,
            `INSERT INTO item (id, name) values ('1', 'a')`,
            `INSERT INTO item (id, name) values ('2', 'b')`,
            `INSERT INTO item (id, name) values ('3', 'c')`,
        ])

        it('remove by passing an entity', async function () {
            let store = await createStore()
            await store.delete(Item, '1')
            await expect(getItemIds(store)).resolves.toEqual(['2', '3'])
        })

        it('remove by passing an array of entities', async function () {
            let store = await createStore()
            await store.delete(Item, ['1', '3'])
            await expect(getItemIds(store)).resolves.toEqual(['2'])
        })

        it('remove by passing an id', async function () {
            let store = await createStore()
            await store.delete(Item, '1')
            await expect(getItemIds(store)).resolves.toEqual(['2', '3'])
        })

        it('remove by passing an array of ids', async function () {
            let store = await createStore()
            await store.delete(Item, ['1', '2'])
            await expect(getItemIds(store)).resolves.toEqual(['3'])
        })
    })

    describe('Update with un-fetched reference', function () {
        useDatabase([
            `CREATE TABLE item (id text primary key , name text)`,
            `CREATE TABLE "order" (id text primary key, item_id text REFERENCES item, qty int4)`,
            `INSERT INTO item (id, name) values ('1', 'a')`,
            `INSERT INTO "order" (id, item_id, qty) values ('1', '1', 3)`,
            `INSERT INTO item (id, name) values ('2', 'b')`,
            `INSERT INTO "order" (id, item_id, qty) values ('2', '2', 3)`,
        ])

        it("auto upsert does not clear reference (single row update)", async function () {
            let store = await createStore()
            let order = assertNotNull(await store.get(Order, '1'))
            order.qty = 5
            let newOrder = await store.findOneOrFail(Order, {
                where: {id: Equal('1')},
                relations: {
                    item: true,
                },
            })
            expect(newOrder.qty).toEqual(5)
            expect(newOrder.item.id).toEqual('1')
        })

        it("auto upsert does not clear reference (multi row update)", async function () {
            let store = await createStore()
            // Load items before orders so a sync does not drop Order ids from the touch set before mutation.
            let items = await store.find(Item, {order: {id: 'ASC'}})
            let orders = await store.find(Order, {order: {id: 'ASC'}})

            orders[0].qty = 5
            orders[1].qty = 1
            orders[1].item = items[0]

            let newOrders = await store.find(Order, {
                relations: {
                    item: true,
                },
                order: {id: 'ASC'},
            })

            expect(newOrders).toEqual([
                {
                    id: '1',
                    item: {
                        id: '1',
                        name: 'a',
                    },
                    qty: 5,
                },
                {
                    id: '2',
                    item: {
                        id: '1',
                        name: 'a',
                    },
                    qty: 1,
                },
            ])
        })
    })
})

export async function createStore(): Promise<Store> {
    const em = await getEntityManager()
    return new Store({
        em,
        state: new StateManager({connection: em.connection}),
        postponeWriteOperations: true,
        cacheEntities: true,
    })
}

export async function getItems(store: Store): Promise<Item[]> {
    return store.find(Item, {where: {}})
}

export function getItemIds(store: Store): Promise<string[]> {
    return getItems(store).then((items) => items.map((it) => it.id).sort())
}
