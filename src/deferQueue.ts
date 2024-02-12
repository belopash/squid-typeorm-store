import {Entity} from '@subsquid/typeorm-store'
import {EntityManager, EntityTarget, FindOptionsRelations} from 'typeorm'
import {mergeRelataions} from './utils'

export type DeferData = {
    ids: Set<string>
    relations: FindOptionsRelations<any>
}

export class DeferQueue {
    private deferMap: Map<EntityTarget<any>, DeferData> = new Map()

    constructor(private em: () => EntityManager) {}

    add<E extends Entity>(entityClass: EntityTarget<E>, id: string, relations?: FindOptionsRelations<E>) {
        const data = this.getData(entityClass)
        data.ids.add(id)

        if (relations != null) {
            data.relations = mergeRelataions(data.relations, relations)
        }
    }

    getData(entityClass: EntityTarget<any>) {
        const em = this.em()
        const metadata = em.connection.getMetadata(entityClass)

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
