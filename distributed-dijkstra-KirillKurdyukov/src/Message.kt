package dijkstra.messages

sealed class Message

object Reject : Message()

object Child: Message()

object NoChild: Message()

data class Parent(val dist: Long) : Message()