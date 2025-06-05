import {DataSource, EntityMetadata} from 'typeorm'
import {RelationMetadata} from 'typeorm/metadata/RelationMetadata'

enum NodeState {
    Unvisited,
    Visiting,
    Visited,
}

const CommitOrderSymbol = Symbol('CommitOrder')

export function getMetadatasInCommitOrder(
    connection: DataSource & {
        [CommitOrderSymbol]?: EntityMetadata[]
    }
): EntityMetadata[] {
    let commitOrder = connection[CommitOrderSymbol]
    if (commitOrder != null) return commitOrder

    const nodeState: Record<string, NodeState> = {}

    function visit(node: EntityMetadata) {
        if (nodeState[node.name] && nodeState[node.name] !== NodeState.Unvisited) return

        nodeState[node.name] = NodeState.Visiting

        for (const edge of node.relations) {
            if (edge.foreignKeys.length === 0) continue

            const target = edge.inverseEntityMetadata

            switch (nodeState[target.name]) {
                case undefined:
                case NodeState.Unvisited: {
                    visit(target)
                    break
                }
                case NodeState.Visiting: {
                    const reversedEdge = target.relations.find((r) => r.inverseEntityMetadata === node)
                    if (reversedEdge != null) {
                        const edgeWeight = getWeight(edge)
                        const reversedEdgeWeight = getWeight(reversedEdge)

                        if (edgeWeight > reversedEdgeWeight) {
                            for (const r of target.relations) {
                                visit(r.inverseEntityMetadata)
                            }

                            nodeState[target.name] = NodeState.Visited
                            commitOrder?.push(target)
                        }
                    }
                    break
                }
            }
        }

        if (nodeState[node.name] !== NodeState.Visited) {
            nodeState[node.name] = NodeState.Visited
            commitOrder?.push(node)
        }
    }

    connection[CommitOrderSymbol] = commitOrder = []
    for (const node of connection.entityMetadatas) {
        visit(node)
    }

    return commitOrder
}

function getWeight(edge: RelationMetadata) {
    return edge.isNullable ? 0 : 1
}
