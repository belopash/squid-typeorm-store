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

        switch (prevType) {
            case UpdateType.Insert:
            case UpdateType.Upsert:
                throw new Error(`Can not add id ${id} because it is already marked as insert or upsert`)
            case UpdateType.Remove:
                this.map.set(id, UpdateType.Upsert)
                break
            case undefined:
                this.map.set(id, UpdateType.Insert)
                break
        }
    }

    upsert(id: string) {
        const prevType = this.map.get(id)

        switch (prevType) {
            case UpdateType.Insert:
                break
            case UpdateType.Upsert:
            case UpdateType.Remove:
            case undefined:
                this.map.set(id, UpdateType.Upsert)
                break
        }
    }

    remove(id: string) {
        const prevType = this.map.get(id)

        switch (prevType) {
            case UpdateType.Insert:
            case UpdateType.Upsert:
            case UpdateType.Remove:
            case undefined:
                this.map.set(id, UpdateType.Remove)
                break
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
