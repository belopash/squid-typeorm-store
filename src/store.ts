import {
    EntityManager,
    EntityMetadata,
    EntityNotFoundError,
    FindOptionsOrder,
    FindOptionsRelations,
    FindOptionsWhere,
} from 'typeorm'
import {EntityTarget} from 'typeorm/common/EntityTarget'
import {ChangeTracker} from '@subsquid/typeorm-store/lib/hot'
import {ChangeType, StateManager} from './utils/stateManager'
import {Logger} from '@subsquid/logger'
import {createFuture, Future} from '@subsquid/util-internal'
import {EntityLiteral, noNull, splitIntoBatches, traverseEntity} from './utils/misc'
import {ColumnMetadata} from 'typeorm/metadata/ColumnMetadata'
import assert from 'assert'
import {EntityClass} from '@subsquid/typeorm-store'
import {DeferList} from './utils/deferList'

export {EntityTarget, EntityLiteral}

export interface GetOptions<E = any> {
    id: string
    relations?: FindOptionsRelations<E>
    cacheEntities?: boolean
}

/**
 * Defines a special criteria to find specific entity.
 */
export interface FindOneOptions<Entity = any> {
    /**
     * Adds a comment with the supplied string in the generated query.  This is
     * helpful for debugging purposes, such as finding a specific query in the
     * database server's logs, or for categorization using an APM product.
     */
    comment?: string
    /**
     * Simple condition that should be applied to match entities.
     */
    where?: FindOptionsWhere<Entity>[] | FindOptionsWhere<Entity>
    /**
     * Indicates what relations of entity should be loaded (simplified left join form).
     */
    relations?: FindOptionsRelations<Entity>
    /**
     * Order, in which entities should be ordered.
     */
    order?: FindOptionsOrder<Entity>

    cacheEntities?: boolean
    syncEntities?: boolean
}

export interface FindManyOptions<Entity = any> extends FindOneOptions<Entity> {
    /**
     * Offset (paginated) where from entities should be taken.
     */
    skip?: number
    /**
     * Limit (paginated) - max number of entities should be taken.
     */
    take?: number

    cacheEntities?: boolean
    syncEntities?: boolean
}

export interface StoreOptions {
    em: EntityManager
    state: StateManager
    changes?: ChangeTracker
    logger?: Logger
    batchWriteOperations: boolean
    cacheEntities: boolean
    syncEntities: boolean
}

/**
 * Restricted version of TypeORM entity manager for squid data handlers.
 */
export class Store {
    protected em: EntityManager
    protected state: StateManager
    protected defers: DeferList
    protected changes?: ChangeTracker
    protected logger?: Logger

    protected batchWriteOperations: boolean
    protected cacheEntities: boolean
    protected syncEntities: boolean

    protected pendingCommit?: Future<void>
    protected isClosed = false

    constructor({em, changes, logger, state, ...opts}: StoreOptions) {
        this.em = em
        this.changes = changes
        this.logger = logger?.child('store')
        this.state = state
        this.batchWriteOperations = opts.batchWriteOperations
        this.cacheEntities = opts.cacheEntities
        this.syncEntities = opts.syncEntities
        this.defers = new DeferList(this.logger?.child('defer'))
    }

    defer<E extends EntityLiteral>(target: EntityTarget<E>, id: string): DeferredEntity<E>
    defer<E extends EntityLiteral>(target: EntityTarget<E>, options: GetOptions<E>): DeferredEntity<E>
    defer<E extends EntityLiteral>(target: EntityTarget<E>, idOrOptions: string | GetOptions<E>): DeferredEntity<E> {
        const md = this.getEntityMetadata(target)

        const options = parseGetOptions(idOrOptions)
        this.defers.add(md, options.id, options.relations)

        return new DeferredEntity({
            get: async () => this.get(target, options),
            getOrFail: async () => this.getOrFail(target, options),
            getOrInsert: async (create) => this.getOrInsert(target, options, create),
        })
    }

    /**
     * Alias for {@link Store.upsert}
     */
    async save<E extends EntityLiteral>(e: E | E[]): Promise<void> {
        return this.upsert(e)
    }

    /**
     * Upserts a given entity or entities into the database.
     *
     * It always executes a primitive operation without cascades, relations, etc.
     */
    async upsert<E extends EntityLiteral>(e: E | E[]): Promise<void> {
        return await this.performWrite(async () => {
            let entities = Array.isArray(e) ? e : [e]
            if (entities.length == 0) return

            for (const entity of entities) {
                this.state.upsert(entity)
            }
        })
    }

    private getFkSignature(fk: ColumnMetadata[], entity: any): bigint {
        let sig = 0n
        for (let i = 0; i < fk.length; i++) {
            let bit = fk[i].getEntityValue(entity) === undefined ? 0n : 1n
            sig |= bit << BigInt(i)
        }
        return sig
    }

    private async _upsert(metadata: EntityMetadata, entities: EntityLiteral[]): Promise<void> {
        this.logger?.debug(`upsert ${entities.length} ${metadata.name} entities`)
        await this.changes?.trackUpsert(metadata.target as EntityClass<any>, entities)

        let fk = metadata.columns.filter((c) => c.relationMetadata)
        if (fk.length == 0) return this.upsertMany(metadata.target, entities)
        let signatures = entities
            .map((e) => ({entity: e, value: this.getFkSignature(fk, e)}))
            .sort((a, b) => (a.value > b.value ? -1 : b.value > a.value ? 1 : 0))
        let currentSignature = signatures[0].value
        let batch: EntityLiteral[] = []
        for (let s of signatures) {
            if (s.value === currentSignature) {
                batch.push(s.entity)
            } else {
                await this.upsertMany(metadata.target, batch)
                currentSignature = s.value
                batch = [s.entity]
            }
        }
        if (batch.length) {
            await this.upsertMany(metadata.target, batch)
        }
    }

    private async upsertMany(target: EntityTarget<any>, entities: EntityLiteral[]) {
        for (let b of splitIntoBatches(entities, 1000)) {
            await this.em.upsert(target, b as any, ['id'])
        }
    }

    /**
     * Inserts a given entity or entities into the database.
     * Does not check if the entity(s) exist in the database and will fail if a duplicate is inserted.
     *
     * Executes a primitive INSERT operation without cascades, relations, etc.
     */
    async insert<E extends EntityLiteral>(e: E | E[]): Promise<void> {
        return await this.performWrite(async () => {
            const entities = Array.isArray(e) ? e : [e]
            if (entities.length == 0) return

            for (const entity of entities) {
                this.state.insert(entity)
            }
        })
    }

    private async _insert(metadata: EntityMetadata, entities: EntityLiteral[]) {
        this.logger?.debug(`insert ${entities.length} ${metadata.name} entities`)
        await this.changes?.trackInsert(metadata.target as EntityClass<any>, entities)
        await this.insertMany(metadata.target, entities)
    }

    private async insertMany(target: EntityTarget<any>, entities: EntityLiteral[]) {
        for (let b of splitIntoBatches(entities, 1000)) {
            await this.em.insert(target, b)
        }
    }

    /**
     * Deletes a given entity or entities from the database.
     *
     * Executes a primitive DELETE query without cascades, relations, etc.
     */
    async delete<E extends EntityLiteral>(e: E | E[]): Promise<void>
    async delete<E extends EntityLiteral>(target: EntityTarget<E>, id: string | string[]): Promise<void>
    async delete<E extends EntityLiteral>(e: E | E[] | EntityTarget<E>, id?: string | string[]): Promise<void> {
        return await this.performWrite(async () => {
            if (id == null) {
                const entities = Array.isArray(e) ? e : [e as E]
                if (entities.length == 0) return

                for (const entity of entities) {
                    this.state.delete(entity.constructor, entity.id)
                }
            } else {
                const ids = Array.isArray(id) ? id : [id]
                if (ids.length == 0) return

                for (const id of ids) {
                    this.state.delete(e as EntityTarget<E>, id)
                }
            }
        })
    }

    /**
     * Alias for {@link Store.delete}
     */
    remove<E extends EntityLiteral>(e: E | E[]): Promise<void>
    remove<E extends EntityLiteral>(target: EntityTarget<E>, id: string | string[]): Promise<void>
    remove<E extends EntityLiteral>(e: E | E[] | EntityTarget<E>, id?: string | string[]): Promise<void> {
        return this.delete(e as any, id as any)
    }

    private async _delete(metadata: EntityMetadata, ids: string[]) {
        this.logger?.debug(`delete ${metadata.name} ${ids.length} entities`)
        await this.changes?.trackDelete(metadata.target as EntityClass<any>, ids)
        await this.deleteMany(metadata.target, ids)
    }

    private async deleteMany(target: EntityTarget<any>, ids: string[]) {
        for (let b of splitIntoBatches(ids, 50000)) {
            await this.em.delete(target, b)
        }
    }

    async count<E extends EntityLiteral>(target: EntityTarget<E>, options?: FindManyOptions<E>): Promise<number> {
        return await this.performRead(async () => {
            return await this.em.count(target, options)
        }, options)
    }

    async countBy<E extends EntityLiteral>(
        target: EntityTarget<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<number> {
        return await this.count(target, {where})
    }

    async find<E extends EntityLiteral>(target: EntityTarget<E>, options: FindManyOptions<E>): Promise<E[]> {
        return await this.performRead(async () => {
            const {cacheEntities, ...opts} = options

            const res = await this.em.find(target, opts)
            if (cacheEntities ?? this.cacheEntities) {
                for (const e of res) {
                    this.cacheEntity(target, e)
                }
            }

            return res
        }, options)
    }

    async findBy<E extends EntityLiteral>(
        target: EntityTarget<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E[]> {
        return await this.find(target, {where})
    }

    async findOne<E extends EntityLiteral>(
        target: EntityTarget<E>,
        options: FindOneOptions<E>
    ): Promise<E | undefined> {
        return await this.performRead(async () => {
            const {cacheEntities, ...opts} = options

            const res = await this.em.findOne(target, opts).then(noNull)
            if (cacheEntities ?? this.cacheEntities) {
                const idOrEntity = res || getIdFromWhere(options.where)
                this.cacheEntity(target, idOrEntity)
            }

            return res
        }, options)
    }

    async findOneBy<E extends EntityLiteral>(
        target: EntityTarget<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E | undefined> {
        return await this.findOne(target, {where})
    }

    async findOneOrFail<E extends EntityLiteral>(target: EntityTarget<E>, options: FindOneOptions<E>): Promise<E> {
        const res = await this.findOne(target, options)
        if (res == null) throw new EntityNotFoundError(target, options.where)

        return res
    }

    async findOneByOrFail<E extends EntityLiteral>(
        target: EntityTarget<E>,
        where: FindOptionsWhere<E> | FindOptionsWhere<E>[]
    ): Promise<E> {
        const res = await this.findOneBy(target, where)
        if (res == null) throw new EntityNotFoundError(target, where)

        return res
    }

    async get<E extends EntityLiteral>(target: EntityTarget<E>, id: string): Promise<E | undefined>
    async get<E extends EntityLiteral>(target: EntityTarget<E>, options: GetOptions<E>): Promise<E | undefined>
    async get<E extends EntityLiteral>(
        target: EntityTarget<E>,
        idOrOptions: string | GetOptions<E>
    ): Promise<E | undefined> {
        const {id, relations, cacheEntities} = parseGetOptions(idOrOptions)

        let entity = this.state.get<E>(target, id, relations)
        if (entity !== undefined) return noNull(entity)

        return await this.findOne(target, {where: {id} as any, relations, cacheEntities, syncEntities: false})
    }

    async getOrFail<E extends EntityLiteral>(target: EntityTarget<E>, id: string): Promise<E>
    async getOrFail<E extends EntityLiteral>(target: EntityTarget<E>, options: GetOptions<E>): Promise<E>
    async getOrFail<E extends EntityLiteral>(target: EntityTarget<E>, idOrOptions: string | GetOptions<E>): Promise<E> {
        const options = parseGetOptions(idOrOptions)

        let e = await this.get(target, options)
        if (e == null) throw new EntityNotFoundError(target, options.id)

        return e
    }

    async getOrInsert<E extends EntityLiteral>(
        target: EntityTarget<E>,
        id: string,
        create: (id: string) => E | Promise<E>
    ): Promise<E>
    async getOrInsert<E extends EntityLiteral>(
        target: EntityTarget<E>,
        options: GetOptions<E>,
        create: (id: string) => E | Promise<E>
    ): Promise<E>
    async getOrInsert<E extends EntityLiteral>(
        target: EntityTarget<E>,
        idOrOptions: string | GetOptions<E>,
        create: (id: string) => E | Promise<E>
    ): Promise<E> {
        const options = parseGetOptions(idOrOptions)
        let e = await this.get(target, options)

        if (e == null) {
            e = await create(options.id)
            await this.insert(e)
        }

        return e
    }

    /**
     * @deprecated use {@link getOrInsert} instead
     */
    async getOrCreate<E extends EntityLiteral>(
        target: EntityTarget<E>,
        idOrOptions: string | GetOptions<E>,
        create: (id: string) => E | Promise<E>
    ) {
        return this.getOrInsert(target, idOrOptions as any, create)
    }

    reset(): void {
        this.state.reset()
    }

    async sync(): Promise<void> {
        await this.pendingCommit?.promise()

        this.pendingCommit = createFuture()
        try {
            await this.state.performUpdate(async (changeSets) => {
                for (const cs of changeSets) {
                    switch (cs.type) {
                        case ChangeType.Upsert:
                            await this._upsert(cs.metadata, cs.entities)
                            break
                        case ChangeType.Insert:
                            await this._insert(cs.metadata, cs.entities)
                            break
                        case ChangeType.Delete:
                            await this._delete(cs.metadata, cs.ids)
                            break
                    }
                }
            })
        } finally {
            this.pendingCommit.resolve()
            this.pendingCommit = undefined
        }
    }

    async flush(): Promise<void> {
        await this.sync()
        this.reset()
    }

    private async performRead<T>(cb: () => Promise<T>, opts?: {syncEntities?: boolean}): Promise<T> {
        this.assertNotClosed()
        if (opts?.syncEntities ?? this.syncEntities) {
            await this.sync()
        }
        return await cb()
    }

    private async performWrite(cb: () => Promise<void>): Promise<void> {
        this.assertNotClosed()
        await this.pendingCommit?.promise()
        await cb()
        if (!this.batchWriteOperations) {
            await this.sync()
        }
    }

    private assertNotClosed() {
        assert(!this.isClosed, `too late to perform db updates, make sure you haven't forgot to await on db query`)
    }

    private cacheEntity<E extends EntityLiteral>(target: EntityTarget<E>, entityOrId?: E | string) {
        if (entityOrId == null) {
            return
        } else if (typeof entityOrId === 'string') {
            this.state.settle(target, entityOrId)
        } else {
            traverseEntity(this.getEntityMetadata(target), entityOrId, (e, md) => this.state.persist(md.target, e))
        }
    }

    private getEntityMetadata(target: EntityTarget<any>) {
        return this.em.connection.getMetadata(target)
    }
}

function parseGetOptions<E>(idOrOptions: string | GetOptions<E>): GetOptions<E> {
    if (typeof idOrOptions === 'string') {
        return {id: idOrOptions}
    } else {
        return idOrOptions
    }
}

function getIdFromWhere(where?: FindOptionsWhere<EntityLiteral>) {
    return typeof where?.id === 'string' ? where.id : undefined
}

export class DeferredEntity<E extends EntityLiteral> {
    constructor(
        private opts: {
            get: () => Promise<E | undefined>
            getOrFail: () => Promise<E>
            getOrInsert: (create: (id: string) => E | Promise<E>) => Promise<E>
        }
    ) {}

    async get(): Promise<E | undefined> {
        return this.opts.get()
    }

    async getOrFail(): Promise<E> {
        return this.opts.getOrFail()
    }

    async getOrInsert(create: (id: string) => E | Promise<E>): Promise<E> {
        return this.opts.getOrInsert(create)
    }

    /**
     * @deprecated use {@link getOrInsert} instead
     */
    async getOrCreate(create: (id: string) => E | Promise<E>): Promise<E> {
        return this.getOrInsert(create)
    }
}
