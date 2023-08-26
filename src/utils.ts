export function* splitIntoBatches<T>(list: T[], maxBatchSize: number): Generator<T[]> {
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

export function copy<T>(obj: T): T {
    if (typeof obj !== 'object' || obj == null) return obj
    else if (obj instanceof Date) {
        return new Date(obj) as any
    } else if (Array.isArray(obj)) {
        return copyArray(obj) as any
    } else if (obj instanceof Map) {
        return new Map(copyArray(Array.from(obj))) as any
    } else if (obj instanceof Set) {
        return new Set(copyArray(Array.from(obj))) as any
    } else if (ArrayBuffer.isView(obj)) {
        return copyBuffer(obj)
    } else {
        const clone = Object.create(Object.getPrototypeOf(obj))
        for (var k in obj) {
            clone[k] = copy(obj[k])
        }
        return clone
    }
}

function copyBuffer(buf: any) {
    if (buf instanceof Buffer) {
        return Buffer.from(buf)
    }

    return new buf.constructor(buf.buffer.slice(), buf.byteOffset, buf.length)
}

function copyArray(arr: any[]) {
    const clone = new Array(arr.length)
    for (let i = 0; i < arr.length; i++) {
        clone[i] = copy(clone[i])
    }
    return clone
}
