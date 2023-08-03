import assert from 'assert'
import {copy} from 'copy-anything'
import {Graph} from 'graph-data-structure'
import {EntityManager, EntityMetadata, FindOptionsRelations, FindOptionsWhere, In} from 'typeorm'
import {EntityClass, FindManyOptions, FindOneOptions, Store, Entity as _Entity} from '@subsquid/typeorm-store'
import {def} from '@subsquid/util-internal'
import {ChangeTracker} from '@subsquid/typeorm-store/lib/hot'

export interface Entity extends _Entity {
    [k: string]: any
}

export type DeferData<E extends Entity> = {ids: Set<string>; relations: FindOptionsRelations<E>}
export type CacheMap<E extends Entity> = Map<string, Map<string, E | null>>
export type ChangeMap<E extends Entity> = Map<string, Map<string, E>>

export class StoreWithCache extends Store {
    static create(store: Store) {
        return new StoreWithCache(store['em'], store['changes'])
    }

    private get _em(): EntityManager {
        return this['em']()
    }

    private deferMap: Map<string, DeferData<any>> = new Map()
    private cacheMap: CacheMap<any> = new Map()
    private classes: Map<string, EntityClass<any>> = new Map()

    private insertMap: ChangeMap<any> = new Map()
    private upsertMap: ChangeMap<any> = new Map()

    private constructor(em: () => EntityManager, changes?: ChangeTracker) {
        super(em, changes)
    }

    async insert<E extends _Entity>(entity: E): Promise<void>
    async insert<E extends _Entity>(entities: E[]): Promise<void>
    async insert<E extends _Entity>(e: E | E[]): Promise<void> {
        let entities = Array.isArray(e) ? e : [e]
        if (entities.length == 0) return

        const entityName = entities[0].constructor.name
        const _insertList = this.getInsertList(entityName)
        const _upsertList = this.getUpsertList(entityName)
        const _cacheMap = this.getCacheMap(entityName)
        for (const entity of entities) {
            assert(!_insertList.has(entity.id))
            assert(!_upsertList.has(entity.id))

            let cached = _cacheMap.get(entity.id)
            assert(cached == null)

            cached = await this.cache(entity)
            _insertList.set(cached.id, cached)
        }
    }

    async upsert<E extends _Entity>(entity: E): Promise<void>
    async upsert<E extends _Entity>(entities: E[]): Promise<void>
    async upsert<E extends _Entity>(e: E | E[]): Promise<void> {
        let entities = Array.isArray(e) ? e : [e]
        if (entities.length == 0) return

        const entityName = entities[0].constructor.name
        const _insertList = this.getInsertList(entityName)
        const _upsertList = this.getUpsertList(entityName)
        for (const entity of entities) {
            const cached = await this.cache(entity)
            if (!_insertList.has(cached.id)) {
                _upsertList.set(cached.id, cached)
            }
        }
    }

    async save<E extends _Entity>(entity: E): Promise<void>
    async save<E extends _Entity>(entities: E[]): Promise<void>
    async save<E extends _Entity>(e: E | E[]): Promise<void> {
        return await this.upsert(e as any)
    }

    remove<E extends Entity>(entity: E): Promise<void>
    remove<E extends Entity>(entities: E[]): Promise<void>
    remove<E extends Entity>(entityClass: EntityClass<E>, id: string | string[]): Promise<void>
    async remove(entityClass: any, id?: any): Promise<void> {
        throw new Error('not implemented')
    }

    async count<E extends Entity>(entityClass: EntityClass<E>, options?: FindManyOptions<E>): Promise<number> {
        await this.flush(entityClass)
        return await super.count(entityClass, options)
    }

    async countBy<E extends Entity>(
        entityClass: EntityClass<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<number> {
        await this.flush(entityClass)
        return await super.countBy(entityClass, where)
    }

    async find<E extends Entity>(entityClass: EntityClass<E>, options?: FindManyOptions<E>): Promise<E[]> {
        await this.flush(entityClass)
        const _deferData = this.getDeferData(entityClass.name)
        options = options || {}
        options.relations =
            options.relations != null ? mergeRelataions(options.relations, _deferData.relations) : _deferData.relations
        const res = await super.find(entityClass, options)
        await this.cache(res, options.relations)
        return res
    }

    async findBy<E extends Entity>(
        entityClass: EntityClass<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E[]> {
        await this.flush(entityClass)
        const res = await super.findBy(entityClass, where)
        await this.cache(res)
        return res
    }

    async findOne<E extends Entity>(entityClass: EntityClass<E>, options: FindOneOptions<E>): Promise<E | undefined> {
        await this.flush(entityClass)
        const _deferData = this.getDeferData(entityClass.name)
        options.relations =
            options.relations != null ? mergeRelataions(options.relations, _deferData.relations) : _deferData.relations
        const res = await super.findOne(entityClass, options)
        if (res != null) await this.cache(res, options.relations)
        return res
    }

    async findOneBy<E extends Entity>(
        entityClass: EntityClass<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E | undefined> {
        await this.flush(entityClass)
        const res = await super.findOneBy(entityClass, where)
        if (res != null) await this.cache(res)
        return res
    }

    async findOneOrFail<E extends Entity>(entityClass: EntityClass<E>, options: FindOneOptions<E>): Promise<E> {
        return await this.findOne(entityClass, options).then(assertNotNull)
    }

    async findOneByOrFail<E extends Entity>(
        entityClass: EntityClass<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E> {
        return await this.findOneBy(entityClass, where).then(assertNotNull)
    }

    async get<E extends Entity>(
        entityClass: EntityClass<E>,
        optionsOrId: string | FindOneOptions<E>
    ): Promise<E | undefined> {
        let res: E | undefined
        let relationMask: FindOptionsRelations<E> | undefined
        if (typeof optionsOrId === 'string') {
            await this.load(entityClass)
            const id = optionsOrId
            const _cacheMap = this.getCacheMap(entityClass.name)
            const entity = _cacheMap.get(id)
            if (entity !== undefined) {
                return entity == null ? undefined : (copy(entity) as E)
            } else {
                await this.flush(entityClass)
                res = await super.get(entityClass, id)
            }
        } else {
            relationMask = optionsOrId.relations
            await this.flush(entityClass)
            res = await super.get(entityClass, optionsOrId)
        }

        if (res != null) await this.cache(res, relationMask)
        return res
    }

    async getOrFail<E extends Entity>(
        entityClass: EntityClass<E>,
        optionsOrId: string | FindOneOptions<E>
    ): Promise<E> {
        let e = await this.get(entityClass, optionsOrId)

        let mes = `missing entity ${entityClass.name}`
        if (typeof optionsOrId === 'string') {
            mes += ` with id "${optionsOrId}"`
        }
        assert(e != null, mes)

        return e
    }

    defer<E extends Entity>(
        entityClass: EntityClass<E>,
        id: string,
        relations?: FindOptionsRelations<E>
    ): DeferredEntity<E> {
        this.classes.set(entityClass.name, entityClass)

        if (relations != null) {
            const metadata = this.getMetadata(entityClass)
            this.validateDeferredRelations(metadata, relations)
        }

        const _deferredList = this.getDeferData(entityClass.name)
        _deferredList.ids.add(id)
        _deferredList.relations =
            relations != null ? mergeRelataions(_deferredList.relations, relations) : _deferredList.relations

        return new DeferredEntity(this, entityClass, id)
    }

    private validateDeferredRelations(metadata: EntityMetadata, relations: FindOptionsRelations<any>) {
        for (const relationMetadata of metadata.relations) {
            const mask = relations[relationMetadata.propertyName]
            if (mask == null) continue

            assert(!relationMetadata.isOneToMany, `OneToMany relation fields can not be deferred`)
            assert(!relationMetadata.isOneToOneNotOwner, `OneToOne relation fields of not owner can not be deferred`)

            if (typeof mask === 'object') {
                this.validateDeferredRelations(relationMetadata.inverseEntityMetadata, mask)
            }
        }
    }

    async flush<E extends Entity>(entityClass?: EntityClass<E>): Promise<void> {
        const entityOrder = await this.getTopologicalOrder()

        // let lastEntity: string | undefined
        // if (entityClass != null) {
        //     const metadata = this.getMetadata(entityClass)

        //     if (
        //         metadata.oneToOneRelations.filter((r) => !r.isOwning).length > 0 ||
        //         metadata.oneToManyRelations.length > 0
        //     ) {
        //         lastEntity = undefined
        //     } else {
        //         lastEntity = metadata.name
        //     }
        // }

        for (const name of entityOrder) {
            const inserts = this.getInsertList(name)
            const upserts = this.getUpsertList(name)
            const delayedUpserts = new Map<string, Entity>()

            const metadata = this._em.connection.getMetadata(name)
            const selfRelations = metadata.manyToOneRelations.filter(
                (r) => r.inverseEntityMetadata.name === metadata.name
            )
            if (selfRelations.length > 0) {
                for (const [id, entity] of upserts) {
                    for (const relation of selfRelations) {
                        const value = relation.getEntityValue(entity)
                        if (value != null && inserts.has(value.id)) {
                            delayedUpserts.set(id, entity)
                            upserts.delete(id)
                            break
                        }
                    }
                }
            }

            if (upserts.size > 0) {
                const entities = upserts.values()
                await super.upsert([...entities])
                upserts.clear()
            }

            if (inserts.size > 0) {
                const entities = inserts.values()
                await super.insert([...entities])
                inserts.clear()
            }

            if (delayedUpserts.size > 0) {
                const entities = delayedUpserts.values()
                await super.upsert([...entities])
                inserts.clear()
            }
        }
    }

    private async load<E extends Entity>(entityClass: EntityClass<E>): Promise<void> {
        const _deferData = this.getDeferData<E>(entityClass.name)
        if (_deferData.ids.size === 0) return

        const _cacheMap = this.getCacheMap(entityClass.name)
        for (const id of _deferData.ids) {
            if (_cacheMap.has(id)) continue
            _cacheMap.set(id, null)
        }

        for (let batch of splitIntoBatches([..._deferData.ids], 30000)) {
            await this.find<any>(entityClass, {where: {id: In(batch)}, relations: _deferData.relations})
        }

        this.deferMap.delete(entityClass.name)
    }

    private getMetadata<E>(entityClass: EntityClass<E>): EntityMetadata {
        return this._em.connection.getMetadata(entityClass)
    }

    private cache<E extends Entity>(entity: E, relations?: FindOptionsRelations<any>): E
    private cache<E extends Entity>(entities: E[], relations?: FindOptionsRelations<any>): E[]
    private cache<E extends Entity>(e: E | E[], relations: FindOptionsRelations<any> = {}) {
        const entities = Array.isArray(e) ? e : [e]
        if (entities.length == 0) return

        const _cacheMap = this.getCacheMap(entities[0].constructor.name)
        const cachedEntities: Entity[] = []
        for (const entity of entities) {
            const constructor = entity.constructor as any
            let cachedEntity = _cacheMap.get(entity.id)
            if (cachedEntity == null) {
                cachedEntity = new constructor({id: entity.id}) as Entity
                _cacheMap.set(entity.id, cachedEntity)
            }

            const metadata = this.getMetadata(constructor)
            for (const column of metadata.columns) {
                if (column.relationMetadata) continue

                const propertyName = column.propertyName
                cachedEntity[propertyName] = entity[propertyName]
            }

            for (const relationMetadata of metadata.relations) {
                const relationPropertyName = relationMetadata.propertyName
                if (!(relationPropertyName in entity)) continue

                const relation = entity[relationPropertyName]

                let mask = relations[relationPropertyName]
                mask = mask === true ? {} : mask === false ? undefined : mask

                if (relationMetadata.isOwning) {
                    if (relation == null) {
                        cachedEntity[relationPropertyName] = null
                    } else if (mask != null) {
                        cachedEntity[relationPropertyName] = this.cache(relation, mask)
                    } else if (
                        cachedEntity[relationPropertyName] == null ||
                        cachedEntity[relationPropertyName].id !== relation.id
                    ) {
                        const _relationCacheMap = this.getCacheMap(relation.constructor.name)
                        const relationConstructor = relation.constructor as any
                        let cachedRelation = _relationCacheMap.get(relation.id)
                        if (cachedRelation == null) {
                            cachedRelation = this.cache(new relationConstructor({id: relation.id}))
                        }
                        cachedEntity[relationPropertyName] = cachedRelation
                    }
                } else if (mask != null) {
                    // We also cache these realations, but do not assign them to cached entity,
                    // since we can not garantee that result will be consistent.
                    if (relationMetadata.isOneToMany) {
                        assert(Array.isArray(relation))
                        for (const r of relation) {
                            this.cache(r, mask)
                        }
                    } else if (relationMetadata.isOneToOne) {
                        this.cache(relation, mask)
                    }
                }
            }

            cachedEntities.push(cachedEntity)
        }

        return Array.isArray(e) ? cachedEntities : cachedEntities[0]
    }

    @def
    private async getTopologicalOrder() {
        const graph = Graph()
        for (const metadata of this._em.connection.entityMetadatas) {
            graph.addNode(metadata.name)
            for (const foreignKey of metadata.foreignKeys) {
                if (foreignKey.referencedEntityMetadata === metadata) continue // don't add self-refs

                graph.addEdge(metadata.name, foreignKey.referencedEntityMetadata.name)
            }
        }
        return graph.topologicalSort().reverse()
    }

    private getDeferData<E extends Entity>(name: string): DeferData<E> {
        let list = this.deferMap.get(name)
        if (list == null) {
            list = {ids: new Set(), relations: {}}
            this.deferMap.set(name, list)
        }

        return list
    }

    private getCacheMap<E extends Entity>(name: string): Map<string, E | null> {
        let map = this.cacheMap.get(name)
        if (map == null) {
            map = new Map()
            this.cacheMap.set(name, map)
        }

        return map
    }

    private getInsertList(name: string): Map<string, Entity> {
        let list = this.insertMap.get(name)
        if (list == null) {
            list = new Map()
            this.insertMap.set(name, list)
        }

        return list
    }

    private getUpsertList(name: string): Map<string, Entity> {
        let list = this.upsertMap.get(name)
        if (list == null) {
            list = new Map()
            this.upsertMap.set(name, list)
        }

        return list
    }
}

function assertNotNull<T>(val: T | null | undefined): T {
    assert(val != null)
    return val
}

function mergeRelataions<E extends Entity>(
    a: FindOptionsRelations<E>,
    b: FindOptionsRelations<E>
): FindOptionsRelations<E> {
    const mergedObject: FindOptionsRelations<E> = {}

    for (const key in a) {
        mergedObject[key] = a[key]
    }

    for (const key in b) {
        const bValue = b[key]
        const value = mergedObject[key]
        if (typeof bValue === 'object') {
            mergedObject[key] = (typeof value === 'object' ? mergeRelataions(value, bValue) : bValue) as any
        } else {
            mergedObject[key] = value || bValue
        }
    }

    return mergedObject
}

export class DeferredEntity<E extends Entity> {
    constructor(private store: StoreWithCache, private entityClass: EntityClass<E>, private id: string) {}

    async get(): Promise<E | undefined> {
        return await this.store.get(this.entityClass, this.id)
    }

    async getOrFail(): Promise<E> {
        return await this.store.getOrFail(this.entityClass, this.id)
    }
}

function* splitIntoBatches<T>(list: T[], maxBatchSize: number): Generator<T[]> {
    if (list.length <= maxBatchSize) {
        yield list
    } else {
        let offset = 0
        while (list.length - offset > maxBatchSize) {
            yield list.slice(offset, offset + maxBatchSize)
            offset += maxBatchSize
        }
        yield list.slice(offset)
    }
}
