package raft

import raft.Message.*
import kotlin.math.max
import kotlin.math.min

/**
 * Raft algorithm implementation.
 * All functions are called from the single main thread.
 *
 * @author Kurdyukov Kirill
 */
class ProcessImpl(private val env: Environment) : Process {
    private val storage = env.storage
    private val machine = env.machine
    private val nextIndex = IntArray(env.nProcesses)
    private val matchIndex = IntArray(env.nProcesses)
    private val majority = (env.nProcesses + 1) / 2
    private val clientCommands = mutableListOf<Command>()
    private val lastIndex: Int get() = storage.readLastLogId().index

    private var leaderId: Int? = null
    private var stateProcess = State.FOLLOWER
    private var currentTerm: Int = storage.readPersistentState().currentTerm
    private var votedFor: Int? = storage.readPersistentState().votedFor
    private var countVotesForMe = 0
    private var commitIndex = 0
    private var lastApplied = 0

    private enum class State {
        FOLLOWER, CANDIDATE, LEADER
    }

    init {
        env.startTimeout(Timeout.ELECTION_TIMEOUT)
    }

    override fun onTimeout() {
        when (stateProcess) {
            State.FOLLOWER, State.CANDIDATE -> {
                stateProcess = State.CANDIDATE
                votedFor = env.processId
                countVotesForMe = 1
                leaderId = null
                currentTerm++
                storage.writePersistentState(PersistentState(currentTerm, votedFor))
                (1..env.nProcesses).forEach {
                    if (it != env.processId)
                        env.send(it, RequestVoteRpc(currentTerm, storage.readLastLogId()))
                }
                env.startTimeout(Timeout.ELECTION_TIMEOUT)
            }
            State.LEADER -> {
                broadcastHeartbeat()
                env.startTimeout(Timeout.LEADER_HEARTBEAT_PERIOD)
            }
        }
    }

    override fun onMessage(srcId: Int, message: Message) {
        if (message.term > currentTerm) {
            stateProcess = State.FOLLOWER
            env.startTimeout(Timeout.ELECTION_TIMEOUT)
            votedFor = null
        }
        currentTerm = max(message.term, currentTerm)
        storage.writePersistentState(PersistentState(currentTerm, votedFor))

        when (message) {
            is RequestVoteRpc -> onMessageRequestVoteRpc(srcId, message)
            is RequestVoteResult -> onMessageRequestVoteResult(srcId, message)
            is AppendEntryRpc -> onMessageAppendEntryRpc(srcId, message)
            is AppendEntryResult -> onMessageAppendEntryResult(srcId, message)
            is ClientCommandRpc -> onMessageClientCommandRpc(srcId, message)
            is ClientCommandResult -> onMessageClientCommandResult(srcId, message)
        }

        while (lastApplied < commitIndex) {
            val command = storage.readLog(++lastApplied)?.command ?: break

            val result = machine.apply(command)

            if (stateProcess == State.LEADER && storage.readLog(lastApplied)!!.id.term == currentTerm) {
                if (command.processId == env.processId) {
                    env.onClientCommandResult(result)
                } else {
                    env.send(command.processId, ClientCommandResult(currentTerm, result))
                }
            }
        }
        storage.writePersistentState(PersistentState(currentTerm, votedFor))
    }

    override fun onClientCommand(command: Command) {
        when (stateProcess) {
            State.CANDIDATE, State.FOLLOWER -> {
                if (leaderId == null) {
                    clientCommands.add(command)
                } else {
                    env.send(leaderId!!, ClientCommandRpc(currentTerm, command))
                }
            }
            State.LEADER -> {
                val entry = commandToEntry(command)
                storage.appendLogEntry(commandToEntry(command))
                (1..env.nProcesses).forEach {
                    if (it != env.processId && nextIndex[it - 1] == entry.id.index) {
                        env.send(it, createAppendEntry(entry, it))
                    }
                }
            }
        }
    }

    private fun onMessageRequestVoteRpc(srcId: Int, message: RequestVoteRpc) {
        if (message.term < currentTerm) {
            env.send(srcId, RequestVoteResult(currentTerm, false))
            return
        }

        if (votedFor == null && storage.readLastLogId() <= message.lastLogId) {
            env.send(srcId, RequestVoteResult(currentTerm, true))
            votedFor = srcId
        } else if (votedFor == srcId) {
            env.send(srcId, RequestVoteResult(currentTerm, true))
        } else {
            env.send(srcId, RequestVoteResult(currentTerm, false))
        }
    }

    private fun onMessageRequestVoteResult(srcId: Int, message: RequestVoteResult) {
        if (message.term < currentTerm) {
            return
        }

        if (stateProcess == State.CANDIDATE && message.voteGranted) {
            countVotesForMe++
            if (countVotesForMe >= majority) {
                stateProcess = State.LEADER
                nextIndex.fill(storage.readLastLogId().index + 1)
                matchIndex.fill(0)
                leaderId = null
                clientCommands.forEach {
                    storage.appendLogEntry(commandToEntry(it))
                }
                clientCommands.clear()
                broadcastHeartbeat()
                env.startTimeout(Timeout.LEADER_HEARTBEAT_PERIOD)
                return
            }
        }
    }

    private fun onMessageAppendEntryRpc(srcId: Int, message: AppendEntryRpc) {
        if (message.term < currentTerm) {
            env.send(srcId, AppendEntryResult(currentTerm, null))
            return
        }

        if (stateProcess == State.CANDIDATE)
            stateProcess = State.FOLLOWER

        leaderId = srcId

        forwardQueueCommandsLeader(srcId)

        if ((storage.readLog(message.prevLogId.index)?.id ?: START_LOG_ID) != message.prevLogId) {
            env.send(srcId, AppendEntryResult(currentTerm, null))
        } else {
            val lastIndex = message.entry?.id?.index?.also {
                storage.appendLogEntry(message.entry)
            } ?: storage.readLastLogId().index
            env.send(
                srcId,
                AppendEntryResult(
                    currentTerm,
                    lastIndex,
                )
            )

            commitIndex = min(message.leaderCommit, lastIndex)
        }
        env.startTimeout(Timeout.ELECTION_TIMEOUT)
    }

    private fun onMessageAppendEntryResult(srcId: Int, message: AppendEntryResult) {
        if (message.term < currentTerm || stateProcess != State.LEADER) {
            return
        }

        if (message.lastIndex != null) {
            matchIndex[srcId - 1] = message.lastIndex
            nextIndex[srcId - 1] = message.lastIndex + 1

            (storage.readLastLogId().index downTo 1).forEach {
                val count = matchIndex.count { i -> i >= it } + 1

                if (count >= majority &&
                    storage.readLog(it)?.id?.term == currentTerm &&
                    commitIndex < it
                ) {
                    commitIndex = it
                    return
                }
            }
        } else {
            nextIndex[srcId - 1]--
            env.send(srcId, createAppendEntry(storage.readLog(nextIndex[srcId - 1]), srcId))
        }
    }

    private fun onMessageClientCommandRpc(srcId: Int, message: ClientCommandRpc) {
        onClientCommand(message.command)
    }

    private fun onMessageClientCommandResult(leaderId: Int, message: ClientCommandResult) {
        env.onClientCommandResult(message.result)
        this.leaderId = leaderId
        forwardQueueCommandsLeader(leaderId)
    }

    private fun broadcastHeartbeat() {
        (1..env.nProcesses).forEach {
            if (it != env.processId) {
                val entry = if (lastIndex >= nextIndex[it - 1]) storage.readLog(nextIndex[it - 1]) else null

                env.send(it, createAppendEntry(entry, it))
            }
        }
    }

    private fun forwardQueueCommandsLeader(srcId: Int) {
        clientCommands.forEach { env.send(srcId, ClientCommandRpc(currentTerm, it)) }
        clientCommands.clear()
    }

    private fun commandToEntry(command: Command) = LogEntry(LogId(lastIndex + 1, currentTerm), command)

    private fun createAppendEntry(entry: LogEntry?, srcId: Int) = AppendEntryRpc(
        currentTerm,
        storage.readLog(nextIndex[srcId - 1] - 1)?.id ?: START_LOG_ID,
        commitIndex,
        entry
    )
}
