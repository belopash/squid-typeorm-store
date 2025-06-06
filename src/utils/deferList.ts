import {EntityMetadata, FindOptionsRelations, ObjectLiteral} from 'typeorm'
import {mergeRelations} from './misc'
import {Logger} from '@subsquid/logger'

export type DeferData = {
    ids: Set<string>
    relations: FindOptionsRelations<any>
}

export class DeferList {
    private map: Map<EntityMetadata, DeferData> = new Map()

    constructor(private logger?: Logger) {
    }

    add<E extends ObjectLiteral>(metadata: EntityMetadata, id: string, relations?: FindOptionsRelations<E>) {
        const data = this.getData(metadata)
        data.ids.add(id)

        this.logger?.debug(`entity ${metadata.name} ${id} deferred`)

        if (relations != null) {
            data.relations = mergeRelations(data.relations, relations)
        }
    }

    remove(metadata: EntityMetadata, id: string) {
        const data = this.getData(metadata)
        data.ids.delete(id)
    }

    values(): Map<EntityMetadata, DeferData> {
        return new Map(this.map)
    }

    clear(): void {
        this.logger?.debug(`cleared`)
        this.map.clear()
    }

    isEmpty(): boolean {
        return this.map.size === 0
    }

    private getData(metadata: EntityMetadata) {
        let list = this.map.get(metadata)
        if (list == null) {
            list = {ids: new Set(), relations: {}}
            this.map.set(metadata, list)
        }

        return list
    }
}
