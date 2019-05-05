package net.joinu.kademlia

import org.junit.jupiter.api.Test
import java.math.BigInteger
import java.util.*


class BinaryTrieTest {
    @Test
    fun `multiple add and remove operations work okay`() {
        val trie = BinaryTrie(TrieNode(), 20, 256)

        val rng = Random()
        val dataset = (1..1000)
            .map {
                val bytes = ByteArray(256)
                rng.nextBytes(bytes)

                bytes
            }
            .map { BigInteger(1, it) }

        dataset.forEachIndexed { idx, data ->
            assert(trie.addData(data)) { "Unable to add data: $data" }

            if (trie.getNeighborhoods().size > 1)
                trie.getNeighborhoods().forEach {
                    assert(it.size >= 20) { "ADD: Invalid cluster count: ${it.size} at element: $idx" }
                }

            assert(trie.flatten().size == idx + 1) { "ADD: Invalid trie size: ${trie.flatten().size} at element: $idx" }
        }

        println(trie)

        dataset.forEachIndexed { idx, data ->
            assert(trie.removeData(data)) { "Unable to remove data: ${data.toString(2)}" }

            if (trie.getNeighborhoods().size > 1)
                trie.getNeighborhoods().forEach {
                    assert(it.size >= 20) { "REMOVE: Invalid cluster count: ${it.size} at element: $idx" }
                }

            assert(trie.flatten().size == (dataset.size - (idx + 1))) { "REMOVE: Invalid trie size: ${trie.flatten().size} at element: $idx" }
        }
    }
}