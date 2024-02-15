import {Entity} from '@subsquid/typeorm-store'
import {EntityManager, EntityTarget} from 'typeorm'

export enum UpdateType {
    Insert,
    Upsert,
    Remove,
}

export interface Update {
    id: string
    type: UpdateType
}

export class UpdatesTracker {
    private map: Map<EntityTarget<any>, Map<string, UpdateType>> = new Map()

    constructor(private em: () => EntityManager) {}

    get<E extends Entity>(target: EntityTarget<E>, id: string) {
        return this.getUpdates(target).get(id)
    }

    set<E extends Entity>(target: EntityTarget<E>, id: string, type: UpdateType) {
        this.getUpdates(target).set(id, type)
        return this
    }

    insert<E extends Entity>(target: EntityTarget<E>, id: string) {
        const prevType = this.get(target, id)
        switch (prevType) {
            case undefined:
                this.set(target, id, UpdateType.Insert)
                break
            case UpdateType.Remove:
                this.set(target, id, UpdateType.Upsert)
                break
            case UpdateType.Insert:
            case UpdateType.Insert:
                throw new Error(`ID ${id} is already marked as insert or upsert`)
        }
    }

    upsert<E extends Entity>(target: EntityTarget<E>, id: string) {
        const prevType = this.get(target, id)
        switch (prevType) {
            case UpdateType.Insert:
                break
            default:
                this.set(target, id, UpdateType.Upsert)
                break
        }
    }

    remove<E extends Entity>(target: EntityTarget<E>, id: string) {
        const prevType = this.get(target, id)
        switch (prevType) {
            case UpdateType.Insert:
                this.getUpdates(target).delete(id)
                break
            default:
                this.set(target, id, UpdateType.Remove)
        }
    }

    getUpdates<E extends Entity>(target: EntityTarget<E>) {
        const em = this.em()
        const metadata = em.connection.getMetadata(target)

        let map = this.map.get(metadata.target)
        if (map == null) {
            map = new Map()
            this.map.set(metadata.target, map)
        }

        return map
    }

    clear() {
        this.map.clear()
    }
}
