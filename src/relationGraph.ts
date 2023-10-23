import {EntityMetadata} from 'typeorm'
import {RelationMetadata} from 'typeorm/metadata/RelationMetadata'

enum State {
    Unvisited,
    Visiting,
    Visited,
}

export class RelationGraph {
    private nodeState: Record<string, State> = {}
    private saveOrder: EntityMetadata[] = []

    getCommitOrder(entities: EntityMetadata[]): EntityMetadata[] {
        this.saveOrder = []
        this.nodeState = {}

        for (const node of entities) {
            this.nodeState[node.name] = State.Unvisited
        }

        for (const node of entities) {
            this.visit(node)
        }

        return this.saveOrder
    }

    private visit(node: EntityMetadata) {
        if (this.nodeState[node.name] !== State.Unvisited) return

        this.nodeState[node.name] = State.Visiting

        for (const edge of node.relations) {
            if (edge.foreignKeys.length === 0) continue

            const target = edge.inverseEntityMetadata

            switch (this.nodeState[target.name]) {
                case undefined:
                case State.Unvisited: {
                    this.visit(target)
                    break
                }
                case State.Visiting: {
                    const reversedEdge = target.relations.find((r) => r.inverseEntityMetadata == node)
                    if (reversedEdge != null) {
                        const edgeWeigth = getWeight(edge)
                        const reversedEdgeWeight = getWeight(reversedEdge)

                        if (edgeWeigth > reversedEdgeWeight) {
                            for (const r of target.relations) {
                                this.visit(r.inverseEntityMetadata)
                            }
                        }

                        this.nodeState[target.name] = State.Visited
                        this.saveOrder.push(target)
                    }
                    break
                }
            }
        }

        if (this.nodeState[node.name] !== State.Visited) {
            this.nodeState[node.name] = State.Visited
            this.saveOrder.push(node)
        }
    }
}

function getWeight(relation: RelationMetadata) {
    return relation.isNullable ? 0 : 1
}
