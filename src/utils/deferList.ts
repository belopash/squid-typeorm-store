import {Entity} from '@subsquid/typeorm-store'
import {EntityMetadata, EntityTarget, FindOptionsRelations} from 'typeorm'
import {mergeRelataions} from './misc'

export type DeferData = {
    ids: Set<string>
    relations: FindOptionsRelations<any>
}

export class DeferList {
    private deferMap: Map<EntityMetadata, DeferData> = new Map()

    constructor() {}

    add<E extends Entity>(metadata: EntityMetadata, id: string, relations?: FindOptionsRelations<E>) {
        const data = this.getData(metadata)
        data.ids.add(id)

        if (relations != null) {
            data.relations = mergeRelataions(data.relations, relations)
        }
    }

    getData(metadata: EntityMetadata) {
        let list = this.deferMap.get(metadata)
        if (list == null) {
            list = {ids: new Set(), relations: {}}
            this.deferMap.set(metadata, list)
        }

        return list
    }

    values() {
        return [...this.deferMap.entries()]
    }

    clear() {
        this.deferMap.clear()
    }
}
