import {Entity} from '@subsquid/typeorm-store'
import {EntityMetadata, EntityTarget, FindOptionsRelations} from 'typeorm'
import {mergeRelataions} from './utils'

export type DeferData = {
    ids: Set<string>
    relations: FindOptionsRelations<any>
}

export class DeferList {
    private deferMap: Map<EntityTarget<any>, DeferData> = new Map()

    constructor() {}

    add<E extends Entity>(metadata: EntityMetadata, id: string, relations?: FindOptionsRelations<E>) {
        const data = this.getData(metadata)
        data.ids.add(id)

        if (relations != null) {
            data.relations = mergeRelataions(data.relations, relations)
        }
    }

    getData(metadata: EntityMetadata) {
        let list = this.deferMap.get(metadata.target)
        if (list == null) {
            list = {ids: new Set(), relations: {}}
            this.deferMap.set(metadata.target, list)
        }

        return list
    }

    values() {
        return [...this.deferMap.entries()].map(([target, data]) => ({target, data}))
    }

    clear() {
        this.deferMap.clear()
    }
}
