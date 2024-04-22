import {EntityMetadata} from 'typeorm'

export enum UpdateType {
    Insert,
    Upsert,
    Remove,
}

export interface Update {
    id: string
    type: UpdateType
}

export class UpdateMap {
    private map: Map<EntityMetadata, Map<string, UpdateType>> = new Map()

    constructor() {}

    get(metadata: EntityMetadata, id: string) {
        return this.getUpdates(metadata).get(id)
    }

    set(metadata: EntityMetadata, id: string, type: UpdateType) {
        this.getUpdates(metadata).set(id, type)
        return this
    }

    insert(metadata: EntityMetadata, id: string) {
        const prevType = this.get(metadata, id)
        switch (prevType) {
            case undefined:
                this.set(metadata, id, UpdateType.Insert)
                break
            case UpdateType.Remove:
                this.set(metadata, id, UpdateType.Upsert)
                break
            case UpdateType.Insert:
            case UpdateType.Insert:
                throw new Error(`ID ${id} is already marked as insert or upsert`)
        }
    }

    upsert(metadata: EntityMetadata, id: string) {
        const prevType = this.get(metadata, id)
        switch (prevType) {
            case UpdateType.Insert:
                break
            default:
                this.set(metadata, id, UpdateType.Upsert)
                break
        }
    }

    remove(metadata: EntityMetadata, id: string) {
        const prevType = this.get(metadata, id)
        switch (prevType) {
            case UpdateType.Insert:
                this.getUpdates(metadata).delete(id)
                break
            default:
                this.set(metadata, id, UpdateType.Remove)
        }
    }

    getUpdates(metadata: EntityMetadata) {
        let map = this.map.get(metadata)
        if (map == null) {
            map = new Map()
            this.map.set(metadata, map)
        }

        return map
    }

    clear() {
        this.map.clear()
    }
}
