import {IsolationLevel, TypeormDatabase, TypeormDatabaseOptions} from '@subsquid/typeorm-store'
import {ChangeTracker} from '@subsquid/typeorm-store/lib/hot'
import {FinalTxInfo, HotTxInfo, HashAndHeight} from '@subsquid/typeorm-store/lib/interfaces'
import assert from 'assert'
import {EntityManager} from 'typeorm'
import {StoreWithCache} from './store'

export {IsolationLevel, TypeormDatabaseOptions}

// @ts-ignore
export class TypeormDatabaseWithCache extends TypeormDatabase {
    // @ts-ignore
    transact(info: FinalTxInfo, cb: (store: StoreWithCache) => Promise<void>): Promise<void> {
        return super.transact(info, cb as any)
    }

    // @ts-ignore
    transactHot(info: HotTxInfo, cb: (store: StoreWithCache, block: HashAndHeight) => Promise<void>): Promise<void> {
        return super.transactHot(info, cb as any)
    }

    private async performUpdates(
        cb: (store: StoreWithCache) => Promise<void>,
        em: EntityManager,
        changeTracker?: ChangeTracker
    ): Promise<void> {
        let running = true

        let store = new StoreWithCache(() => {
            assert(running, `too late to perform db updates, make sure you haven't forgot to await on db query`)
            return em
        }, changeTracker)

        try {
            await cb(store)
        } finally {
            running = false
        }
    }
}
