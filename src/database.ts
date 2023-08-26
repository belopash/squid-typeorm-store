import {IsolationLevel, Store, TypeormDatabase, TypeormDatabaseOptions} from '@subsquid/typeorm-store'
import {ChangeTracker} from '@subsquid/typeorm-store/lib/hot'
import assert from 'assert'
import {EntityManager} from 'typeorm'

export {IsolationLevel, TypeormDatabaseOptions}

// @ts-ignore
export class TypeormDatabaseWithCache extends TypeormDatabase {
    private async performUpdates(
        cb: (store: Store) => Promise<void>,
        em: EntityManager,
        changeTracker?: ChangeTracker
    ): Promise<void> {
        let running = true

        let store = new Store(() => {
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
