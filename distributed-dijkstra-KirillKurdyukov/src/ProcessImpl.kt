package dijkstra

import dijkstra.messages.*
import dijkstra.system.environment.Environment

class ProcessImpl(private val environment: Environment) : Process {
    private var distance: Long? = null
    private var childCount = 0
    private var balance = 0
    private var parentId = -1

    private fun dist(): Long = if (distance == null) Long.MAX_VALUE else distance!!

    private fun multicast() = environment.neighbours.forEach { (srcId, dist) ->
        balance++
        environment.send(srcId, Parent(dist + distance!!))
    }

    private fun isFinish() {
        if (balance == 0 && childCount == 0)
            when (parentId) {
                -1 -> environment.finishExecution()
                else -> {
                    environment.send(parentId, NoChild);
                    parentId = -1
                }
            }
    }

    override fun onMessage(srcId: Int, message: Message) {
        when (message) {
            is Reject -> balance--
            is Child -> {
                balance--
                childCount++
            }
            is NoChild -> childCount--
            is Parent -> {
                if (dist() <= message.dist) {
                    environment.send(srcId, Reject)
                    return
                } else {
                    if (parentId != -1) {
                        environment.send(parentId, NoChild)
                    }
                    parentId = srcId
                    distance = message.dist
                    environment.send(srcId, Child)
                    multicast()
                }
            }
        }
        isFinish()
    }

    override fun getDistance(): Long? = distance

    override fun startComputation() {
        distance = 0
        multicast()
        isFinish()
    }
}