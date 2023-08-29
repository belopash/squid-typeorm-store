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
    private map: Map<string, UpdateType> = new Map()

    get(id: string) {
        return this.map.get(id)
    }

    insert(id: string) {
        const prevType = this.map.get(id)
        if (prevType === UpdateType.Insert || prevType === UpdateType.Upsert) {
            throw new Error(`ID ${id} is already marked as insert or upsert`)
        }
        this.map.set(id, UpdateType.Insert)
    }

    upsert(id: string) {
        this.map.set(id, UpdateType.Upsert)
    }

    remove(id: string) {
        const prevType = this.map.get(id)
        if (prevType == UpdateType.Insert) {
            this.map.delete(id)
        } else {
            this.map.set(id, UpdateType.Remove)
        }
    }

    clear() {
        this.map.clear()
    }

    *[Symbol.iterator](): IterableIterator<Update> {
        for (const [id, type] of this.map) {
            yield {
                id,
                type,
            }
        }
    }
}
