import system.DataHolderEnvironment
import java.lang.Integer.min

class DataHolderImpl<T : Comparable<T>>(
    private val keys: List<T>,
    dataHolderEnvironment: DataHolderEnvironment
) : DataHolder<T> {
    private val step = dataHolderEnvironment.batchSize
    private var prevL = 0
    private var l = 0
        get() = min(field, keys.size)

    private val r
        get() = min(l + step, keys.size)

    override fun checkpoint() { prevL = l }

    override fun rollBack() { l = prevL }

    override fun getBatch(): List<T> = keys.subList(l, r).also { l += step }
}