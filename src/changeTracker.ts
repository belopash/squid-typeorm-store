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
        if (prevType === UpdateType.Insert || prevType === UpdateType.Upsert) {
            throw new Error(`ID ${id} is already marked as insert or upsert`)
        } else {
            this.set(target, id, UpdateType.Insert)
        }
    }

    upsert<E extends Entity>(target: EntityTarget<E>, id: string) {
        const prevType = this.get(target, id)
        if (prevType === UpdateType.Insert) {
        } else {
            this.set(target, id, UpdateType.Upsert)
        }
    }

    remove<E extends Entity>(target: EntityTarget<E>, id: string) {
        const prevType = this.get(target, id)
        if (prevType == UpdateType.Insert) {
            this.getUpdates(target).delete(id)
        } else {
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
