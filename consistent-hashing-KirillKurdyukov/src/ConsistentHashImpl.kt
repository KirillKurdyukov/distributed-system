class ConsistentHashImpl<K> : ConsistentHash<K> {
    /**
     * Our circle hashes
     */
    private var hashHolder: List<Pair<Int, Shard>> = mutableListOf()

    /**
     * It is guaranteed that at least one node exists in the system at the time of this call.
     */
    override fun getShardByKey(key: K): Shard {
        var l: Int = -1;
        var r: Int = hashHolder.size
        val hash = key.hashCode()
        while (r - l > 1) {
            val m = (r + l) / 2

            if (hashHolder[m].first + 1 <= hash) {
                l = m
            } else {
                r = m
            }
        }

        return hashHolder[circleInd(r)].second
    }

    override fun addShard(newShard: Shard, vnodeHashes: Set<Int>): Map<Shard, Set<HashRange>> {
        val diff = mutableMapOf<Shard, Set<HashRange>>()
        val vnodeHashesSorted = vnodeHashes.sorted()

        if (vnodeHashesSorted.isEmpty()) return diff
        if (hashHolder.isEmpty()) {
            hashHolder = vnodeHashesSorted.map { Pair(it, newShard) }
            return diff
        }

        var (itLeft, itRight) = listOf(0, 0)
        val newHashHolder = mutableListOf<Pair<Int, Shard>>()

        for (i in 0 until hashHolder.size + vnodeHashesSorted.size) {
            val elemLeft = if (itLeft == hashHolder.size) Int.MAX_VALUE else hashHolder[itLeft].first
            val elemRight = if (itRight == vnodeHashesSorted.size) Int.MAX_VALUE else vnodeHashesSorted[itRight]

            if (elemLeft < elemRight) {
                newHashHolder.add(hashHolder[itLeft++])
            } else {
                newHashHolder.add(Pair(vnodeHashesSorted[itRight++], newShard))
            }
        }

        hashHolder = newHashHolder

        var leftBoarder: Int = lastBoarder { it.first !in vnodeHashes }
        for (i in hashHolder.indices) {
            val prev = circleInd(i - 1)
            val next = circleInd(i + 1)
            if (hashHolder[prev].first !in vnodeHashes && hashHolder[i].first in vnodeHashes) {
                leftBoarder = hashHolder[prev].first + 1
            }
            if (hashHolder[i].first in vnodeHashes && hashHolder[next].first !in vnodeHashes) {
                addedSegment(diff, hashHolder[next].second, leftBoarder, hashHolder[i].first)
            }
        }
        return diff
    }

    override fun removeShard(shard: Shard): Map<Shard, Set<HashRange>> {
        val diff = mutableMapOf<Shard, Set<HashRange>>()
        val newHashHolder = mutableListOf<Pair<Int, Shard>>()

        var leftBoarder: Int = lastBoarder { it.second != shard }
        hashHolder.forEachIndexed { i, it ->
            if (it.second != shard) newHashHolder.add(it) else {
                val prev = circleInd(i - 1)
                val next = circleInd(i + 1)
                if (hashHolder[prev].second != shard && hashHolder[i].second == shard) {
                    leftBoarder = hashHolder[prev].first + 1
                }
                if (hashHolder[i].second == shard && hashHolder[next].second != shard) {
                    addedSegment(diff, hashHolder[next].second, leftBoarder, hashHolder[i].first)
                }
            }
        }

        hashHolder = newHashHolder
        return diff
    }

    private fun circleInd(i: Int, circle: List<*> = hashHolder) = if (i >= 0) {
        if (i < circle.size) i else 0
    } else circle.size - 1

    private fun addedSegment(
        diff: MutableMap<Shard, Set<HashRange>>, currentShard: Shard, leftBoarder: Int, rightBoarder: Int
    ) = diff.merge(currentShard, mutableSetOf(HashRange(leftBoarder, rightBoarder))) { oldVal, newVal ->
        (oldVal as MutableSet).apply { addAll(newVal) }
    }

    private fun lastBoarder(predicate: (Pair<Int, Shard>) -> Boolean): Int {
        for (i in hashHolder.size - 1 downTo 0) {
            if (predicate(hashHolder[i]))
                return hashHolder[i].first + 1
        }
        throw RuntimeException("Impossible!")
    }
}