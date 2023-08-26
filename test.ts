class A {
    a = 1
    b = new Uint8Array([1, 2, 3, 4, 5])
    c = 3

    log() {
        console.log(this.a, this.b)
    }
}

class B extends A {
    a2 = 0
}

function main() {
    const a = {a: 'hello'}
    const b = copy(a)
    b.a = 'a'
    // b.b.reverse()

    console.log(a, b)
    // a.log()
    // b.log()
}

main()

function copyBuffer(buf: any) {
    if (buf instanceof Buffer) {
        return Buffer.from(buf)
    }

    return new buf.constructor(buf.buffer.slice(), buf.byteOffset, buf.length)
}

function copyArray(arr: any[]) {
    var clone = new Array(arr.length)
    for (var i = 0; i < arr.length; i++) {
        clone[i] = copy(clone[i])
    }
    return clone
}

function copy<T>(obj: T): T {
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
