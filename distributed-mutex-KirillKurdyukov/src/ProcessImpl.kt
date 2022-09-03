package mutex

import java.util.*

/**
 * Distributed mutual exclusion implementation.
 * All functions are called from the single main thread.
 *
 * @author Kirill Kurdyukov
 */
class ProcessImpl(private val env: Environment) : Process {
    /**
     * all forks are dirty
     */
    private val isDirtyFork = BooleanArray(env.nProcesses + 1) { true }

    /**
     * Start graph
     *
     *   2 -> 3
     *   /|   \
     *   |    \|
     *   1 -> 4
     * haveFork[env.processId] is my process go to CS
     */
    private val haveFork = BooleanArray(env.nProcesses + 1) { it > env.processId }
    private val queue = LinkedList<Int>()
    private var inCS = false

    override fun onMessage(srcId: Int, message: Message) {
        message.parse {
            when (readEnum<MsgType>()) {
                MsgType.REQ -> {
                    if (isDirtyFork[srcId]) {
                        giveForkAway(srcId)
                    } else {
                        queue.add(srcId)
                    }
                }
                MsgType.OK -> {
                    haveFork[srcId] = true
                    isDirtyFork[srcId] = false
                }
            }
            checkCSEnter()
        }
    }

    private fun giveForkAway(srcId: Int) {
        haveFork[srcId] = false
        isDirtyFork[srcId] = false
        send(srcId, MsgType.OK)
        if (haveFork[env.processId])
            send(srcId, MsgType.REQ)
    }

    override fun onLockRequest() {
        check(!inCS) { "Lock was already requested" }
        haveFork[env.processId] = true

        multicast()
        checkCSEnter()
    }

    override fun onUnlockRequest() {
        check(inCS) { "We are not in critical section" }

        haveFork[env.processId] = false
        for (i in 1 .. env.nProcesses) { isDirtyFork[i] = true }
        inCS = false

        env.unlocked()
        queue.forEach { giveForkAway(it) }

        queue.clear()
    }

    private fun multicast() {
        for (i in 1..env.nProcesses) {
            if (!haveFork[i]) {
                send(i, MsgType.REQ)
            }
        }
    }

    private fun send(id: Int, msgType: MsgType) {
        env.send(id) {
            writeEnum(msgType)
        }
    }

    private fun checkCSEnter() {
        if (inCS) return // already in CS, do nothing

        for (i in 1 .. env.nProcesses) { if (!haveFork[i]) return }// don't have fork
        for (i in 1 .. env.nProcesses) { isDirtyFork[i] = false }

        inCS = true
        env.locked()
    }
    /**
     * REQ - give your fork
     * OK - take my fork
     */
    private enum class MsgType { REQ, OK }
}
