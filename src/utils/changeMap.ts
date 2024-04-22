import {Logger} from '@subsquid/logger'
import {EntityMetadata} from 'typeorm'

export enum ChangeType {
    Insert = 'Insert',
    Upsert = 'Upsert',
    Remove = 'Remove',
}

export interface Change {
    id: string
    type: ChangeType
}

export class ChangeMap {
    private map: Map<EntityMetadata, Map<string, ChangeType>> = new Map()
    private logger: Logger

    constructor(private opts: {logger: Logger}) {
        this.logger = this.opts.logger.child('changes')
    }

    get(metadata: EntityMetadata, id: string) {
        return this.getUpdates(metadata).get(id)
    }

    set(metadata: EntityMetadata, id: string, type: ChangeType) {
        this.getUpdates(metadata).set(id, type)
        this.logger.debug(`entity ${metadata.name} ${id} marked as ${type}`)
        return this
    }

    insert(metadata: EntityMetadata, id: string) {
        const prevType = this.get(metadata, id)
        switch (prevType) {
            case undefined:
                this.set(metadata, id, ChangeType.Insert)
                this.logger.debug(`entity ${metadata.name} ${id} already marked as ${ChangeType.Insert}`)
                break
            case ChangeType.Remove:
                this.set(metadata, id, ChangeType.Upsert)
                break
            case ChangeType.Insert:
            case ChangeType.Upsert:
                throw new Error(
                    `${metadata.name} ${id} is already marked as ${ChangeType.Insert} or ${ChangeType.Upsert}`
                )
        }
    }

    upsert(metadata: EntityMetadata, id: string) {
        const prevType = this.get(metadata, id)
        switch (prevType) {
            case ChangeType.Insert:
                this.logger.debug(`entity ${metadata.name} ${id} already marked as ${ChangeType.Insert}`)
                break
            case ChangeType.Upsert:
                this.logger.debug(`entity ${metadata.name} ${id} already marked as ${ChangeType.Upsert}`)
                break
            default:
                this.set(metadata, id, ChangeType.Upsert)
                break
        }
    }

    remove(metadata: EntityMetadata, id: string) {
        const prevType = this.get(metadata, id)
        switch (prevType) {
            case ChangeType.Insert:
                this.getUpdates(metadata).delete(id)
                break
            case ChangeType.Remove:
                this.logger.debug(`entity ${metadata.name} ${id} already marked as ${ChangeType.Remove}`)
                break
            default:
                this.set(metadata, id, ChangeType.Remove)
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
        this.logger.debug(`cleared`)
        this.map.clear()
    }
}
