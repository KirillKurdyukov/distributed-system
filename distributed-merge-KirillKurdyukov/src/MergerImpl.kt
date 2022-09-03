import system.MergerEnvironment
import java.util.*

class MergerImpl<T : Comparable<T>>(
    private val mergerEnvironment: MergerEnvironment<T>,
    prevStepBatches: Map<Int, List<T>>?
) : Merger<T> {
    private val holderToBatch: MutableMap<Int, List<T>> = if (prevStepBatches == null)
        (0 until mergerEnvironment.dataHoldersCount)
            .map { Pair(it, mergerEnvironment.requestBatch(it)) }
            .filter { it.second.isNotEmpty() }
            .toMap().toMutableMap()
    else {
        val newMap = prevStepBatches.toMutableMap()
        val setKeys = (0 until mergerEnvironment.dataHoldersCount)
            .toSet()
            .minus(prevStepBatches.keys)

        setKeys.forEach {
            val listHolder = mergerEnvironment.requestBatch(it)
            if (listHolder.isNotEmpty()) newMap[it] = listHolder
        }

        newMap
    }
    private val priorityQueue =
        PriorityQueue<Pair<T, Int>> { v, u -> v.first.compareTo(u.first) }.apply {
            holderToBatch.toList().forEach {
                val iter = it.second.first()
                add(Pair(iter, it.first))
            }
        }

    override fun mergeStep(): T? = if (priorityQueue.isEmpty()) null else {
        val pair = priorityQueue.remove()
        val list = holderToBatch[pair.second]!!
        val newList = list.subList(1, list.size)
        if (newList.isNotEmpty()) {
            holderToBatch[pair.second] = newList
            priorityQueue.add(Pair(newList.first(), pair.second))
        } else {
            val holder = mergerEnvironment.requestBatch(pair.second)
            if (holder.isNotEmpty()) {
                holderToBatch[pair.second] = holder
                priorityQueue.add(Pair(holder.first(), pair.second))
            } else {
                holderToBatch.remove(pair.second)
            }
        }
        pair.first
    }

    override fun getRemainingBatches(): Map<Int, List<T>> = holderToBatch
}