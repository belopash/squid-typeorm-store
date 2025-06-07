import {Logger} from '@subsquid/logger'
import {unexpectedCase} from '@subsquid/util-internal'
import assert from 'assert'
import clone from 'fast-copy'
import {DataSource, EntityMetadata, EntityTarget, FindOptionsRelations} from 'typeorm'
import {CacheMap} from './cacheMap'
import {EntityLiteral} from './misc'
import {getMetadatasInCommitOrder} from './commitOrder'

export enum ChangeType {
    Insert = 'insert',
    Upsert = 'upsert',
    Delete = 'delete',
}

export type InsertChangeSet = {type: ChangeType.Insert; metadata: EntityMetadata; entities: EntityLiteral[]}
export type UpsertChangeSet = {type: ChangeType.Upsert; metadata: EntityMetadata; entities: EntityLiteral[]}
export type DeleteChangeSet = {type: ChangeType.Delete; metadata: EntityMetadata; ids: string[]}

export type ChangeSet = InsertChangeSet | UpsertChangeSet | DeleteChangeSet

export class StateManager {
    protected connection: DataSource
    protected cacheMap: CacheMap
    protected stateMap: Map<EntityMetadata, Map<string, ChangeType>>
    protected commitOrder: EntityMetadata[]
    protected commitOrderMap: Map<EntityMetadata, number>
    protected logger?: Logger

    constructor({connection, logger}: {connection: DataSource; logger?: Logger}) {
        this.connection = connection
        this.logger = logger
        this.cacheMap = new CacheMap(this.logger?.child('cache'))
        this.stateMap = new Map()
        this.commitOrder = getMetadatasInCommitOrder(connection)
        // Pre-compute commit order indices for O(1) lookup
        this.commitOrderMap = new Map()
        this.commitOrder.forEach((metadata, index) => {
            this.commitOrderMap.set(metadata, index)
        })
    }

    get<E extends EntityLiteral>(
        target: EntityTarget<any>,
        id: string,
        relationMask?: FindOptionsRelations<any>
    ): E | null | undefined {
        const metadata = this.connection.getMetadata(target)
        const cached = this.cacheMap.get(metadata, id)

        if (cached == null) {
            return undefined
        } else if (cached.value == null) {
            return null
        } else {
            const entity = cached.value
            const clonedEntity = metadata.create()

            for (const column of metadata.nonVirtualColumns) {
                const objectColumnValue = column.getEntityValue(entity)
                if (objectColumnValue !== undefined) {
                    column.setEntityValue(clonedEntity, clone(objectColumnValue))
                }
            }

            if (relationMask != null) {
                for (const relation of metadata.relations) {
                    const inverseMask = relationMask[relation.propertyName]
                    if (!inverseMask) continue

                    const inverseEntityMock = relation.getEntityValue(entity) as EntityLiteral

                    if (inverseEntityMock === null) {
                        relation.setEntityValue(clonedEntity, null)
                    } else {
                        const cachedInverseEntity =
                            inverseEntityMock != null
                                ? this.get(
                                      relation.inverseEntityMetadata.target,
                                      inverseEntityMock.id,
                                      typeof inverseMask === 'boolean' ? undefined : inverseMask
                                  )
                                : undefined

                        if (cachedInverseEntity === undefined) {
                            return undefined // unable to build whole relation chain
                        } else {
                            relation.setEntityValue(clonedEntity, cachedInverseEntity)
                        }
                    }
                }
            }

            return clonedEntity
        }
    }

    insert(entity: EntityLiteral): void {
        const metadata = this.connection.getMetadata(entity.constructor)
        const prevType = this.getState(metadata, entity.id)
        switch (prevType) {
            case undefined:
                this.setState(metadata, entity.id, ChangeType.Insert)
                this.cacheMap.add(metadata, entity, {override: true, nullify: true})
                break
            case ChangeType.Insert:
            case ChangeType.Upsert:
                throw new Error(`Entity ${metadata.name} ${entity.id} is already marked as ${prevType}`)
            case ChangeType.Delete:
                this.setState(metadata, entity.id, ChangeType.Upsert)
                this.cacheMap.add(metadata, entity, {override: true, nullify: true})
                break
            default:
                throw unexpectedCase(prevType)
        }
    }

    upsert(entity: EntityLiteral): void {
        const metadata = this.connection.getMetadata(entity.constructor)
        const prevType = this.getState(metadata, entity.id)
        switch (prevType) {
            case undefined:
            case ChangeType.Upsert:
                this.setState(metadata, entity.id, ChangeType.Upsert)
                this.cacheMap.add(metadata, entity, {override: true})
                break
            case ChangeType.Insert:
                this.cacheMap.add(metadata, entity, {override: true})
                break
            case ChangeType.Delete:
                this.setState(metadata, entity.id, ChangeType.Upsert)
                this.cacheMap.add(metadata, entity, {nullify: true, override: true})
                break
            default:
                throw unexpectedCase(prevType)
        }
    }

    delete(target: EntityTarget<any>, id: string): void {
        const metadata = this.connection.getMetadata(target)
        const prevType = this.getState(metadata, id)
        switch (prevType) {
            case undefined:
            case ChangeType.Upsert:
            case ChangeType.Insert:
                this.setState(metadata, id, ChangeType.Delete)
                this.cacheMap.delete(metadata, id)
                break
            case ChangeType.Delete:
                this.logger?.debug(`entity ${metadata.name} ${id} is already marked as ${ChangeType.Delete}`)
                break
            default:
                throw unexpectedCase(prevType)
        }
    }

    persist(target: EntityTarget<any>, entity: EntityLiteral | string) {
        const metadata = this.connection.getMetadata(target)
        if (typeof entity === 'string') {
            this.cacheMap.settle(metadata, entity)
        } else {
            this.getChanges(metadata).delete(entity.id) // reset state
            this.cacheMap.add(metadata, entity)
        }
    }

    isInserted(target: EntityTarget<any>, id: string) {
        const metadata = this.connection.getMetadata(target)
        return this.getState(metadata, id) === ChangeType.Insert
    }

    isUpserted(target: EntityTarget<any>, id: string) {
        const metadata = this.connection.getMetadata(target)
        return this.getState(metadata, id) === ChangeType.Upsert
    }

    isDeleted(target: EntityTarget<any>, id: string) {
        const metadata = this.connection.getMetadata(target)
        return this.getState(metadata, id) === ChangeType.Delete
    }

    isExists(target: EntityTarget<any>, id: string) {
        const metadata = this.connection.getMetadata(target)
        return this.cacheMap.has(metadata, id)
    }

    reset(): void {
        this.logger?.debug(`reset`)
        this.stateMap.clear()
        this.cacheMap.clear()
    }

    isEmpty(): boolean {
        return this.stateMap.size === 0
    }

    async performUpdate(cb: (cs: ChangeSet[]) => Promise<void>) {
        if (this.isEmpty()) return

        const inserts: ChangeSet[] = []
        const upserts: ChangeSet[] = []
        const deletes: ChangeSet[] = []
        const extraUpserts: ChangeSet[] = []

        for (const metadata of this.commitOrder) {
            const entityChanges = this.stateMap.get(metadata)
            if (entityChanges == null || entityChanges.size == 0) continue

            const changes = {
                inserts: [] as EntityLiteral[],
                upserts: [] as EntityLiteral[],
                deletes: [] as string[],
                extraUpserts: [] as EntityLiteral[],
            }

            for (const [id, type] of entityChanges) {
                const cached = this.cacheMap.get(metadata, id)

                switch (type) {
                    case ChangeType.Insert: {
                        assert(cached?.value != null, `unable to insert entity ${metadata.name} ${id}`)
                        const {entity, extraUpsert} = this.processEntityRelations(cached.value, ChangeType.Insert)

                        changes.inserts.push(entity)
                        if (extraUpsert != null) {
                            changes.extraUpserts.push(extraUpsert)
                        }
                        break
                    }
                    case ChangeType.Upsert: {
                        assert(cached?.value != null, `unable to upsert entity ${metadata.name} ${id}`)
                        const {entity, extraUpsert} = this.processEntityRelations(cached.value, ChangeType.Upsert)

                        changes.upserts.push(entity)
                        if (extraUpsert != null) {
                            changes.extraUpserts.push(extraUpsert)
                        }
                        break
                    }
                    case ChangeType.Delete: {
                        changes.deletes.push(id)
                        break
                    }
                }
            }

            if (changes.inserts.length > 0) {
                inserts.push({type: ChangeType.Insert, metadata, entities: changes.inserts})
            }
            if (changes.upserts.length > 0) {
                upserts.push({type: ChangeType.Upsert, metadata, entities: changes.upserts})
            }
            if (changes.deletes.length > 0) {
                deletes.push({type: ChangeType.Delete, metadata, ids: changes.deletes})
            }
            if (changes.extraUpserts.length > 0) {
                extraUpserts.push({type: ChangeType.Upsert, metadata, entities: changes.extraUpserts})
            }
        }

        await cb([...inserts, ...upserts, ...deletes, ...extraUpserts])

        this.stateMap.clear()
    }

    private processEntityRelations(entity: EntityLiteral, changeType: ChangeType) {
        const metadata = this.connection.getMetadata(entity.constructor)
        const commitOrderIndex = this.commitOrderMap.get(metadata) ?? -1

        let result = entity
        let extraUpsert: EntityLiteral | undefined

        for (const relation of metadata.relations) {
            if (relation.foreignKeys.length == 0) continue

            const inverseMetadata = relation.inverseEntityMetadata
            if (metadata === inverseMetadata) continue

            const inverseEntity = relation.getEntityValue(entity)
            if (inverseEntity == null || inverseEntity.id === entity.id) continue

            const invCommitOrderIndex = this.commitOrderMap.get(inverseMetadata)!

            const isInverseInserted = this.isInserted(inverseMetadata.target, inverseEntity.id)
            const isInverseUpserted = this.isUpserted(inverseMetadata.target, inverseEntity.id)

            let shouldProcess = false
            if (changeType === ChangeType.Insert) {
                shouldProcess = isInverseUpserted || (isInverseInserted && invCommitOrderIndex >= commitOrderIndex)
            } else if (changeType === ChangeType.Upsert) {
                shouldProcess = isInverseUpserted && invCommitOrderIndex >= commitOrderIndex
            }
            if (!shouldProcess) continue

            if (extraUpsert == null) {
                extraUpsert = result
                result = metadata.create() as EntityLiteral
                Object.assign(result, extraUpsert)
            }

            relation.setEntityValue(result, undefined)
        }

        return {entity: result, extraUpsert}
    }

    private setState(metadata: EntityMetadata, id: string, type: ChangeType): this {
        this.getChanges(metadata).set(id, type)
        this.logger?.debug(`entity ${metadata.name} ${id} marked as ${type}`)
        return this
    }

    private getState(metadata: EntityMetadata, id: string): ChangeType | undefined {
        return this.getChanges(metadata).get(id)
    }

    private getChanges(metadata: EntityMetadata): Map<string, ChangeType> {
        let map = this.stateMap.get(metadata)
        if (map == null) {
            map = new Map()
            this.stateMap.set(metadata, map)
        }

        return map
    }
}
