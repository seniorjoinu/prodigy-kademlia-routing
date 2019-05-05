package net.joinu.kademlia

import kotlinx.coroutines.*
import net.joinu.kademlia.addressbook.AddressBook
import net.joinu.kademlia.addressbook.InMemoryBinaryTrieAddressBook
import net.joinu.kademlia.addressbook.getMyCluster
import net.joinu.prodigy.ProtocolRunner
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigInteger
import java.net.InetSocketAddress
import java.util.*

class ClusterTest {
    val k = 3
    val nodesCount = 10

    val rng = Random()

    init {
        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG")
    }

    @Test
    fun `peers contained in a same cluster are always have it in consistent state`() = runBlocking {
        val instances = createInstances(nodesCount, this)

        instances.forEachIndexed { index, (protocol, runner) ->
            if (index == 0) return@forEachIndexed

            // bootstrap with first
            assert(protocol.bootstrap(InetSocketAddress("localhost", 1337)))

            val clusters = instances.map { it.first.addressBook.getMyCluster() }
            val clustersByLabels = clusters.groupBy { it.name }

            val clustersConsistent = clustersByLabels.values.all { clustersWithSameName ->
                val first = clustersWithSameName.first().peers.toSet()
                clustersWithSameName.all { cluster -> cluster.peers.toSet() == first }
            }

            assert(clustersConsistent)
        }

        instances.forEachIndexed { index, (protocol, runner) ->
            if (index == 0) return@forEachIndexed

            protocol.byeMyCluster()

            val clusters = instances.map { it.first.addressBook.getMyCluster() }
            val clustersByLabels = clusters.groupBy { it.name }

            val clustersConsistent = clustersByLabels.values.all { clustersWithSameName ->
                val first = clustersWithSameName.first().peers.toSet()
                clustersWithSameName.all { cluster -> cluster.peers.toSet() == first }
            }

            assert(clustersConsistent)
        }

        instances.forEach { it.second.close() }
    }

    @Test
    fun `peers are able to ping each other`() = runBlocking {
        val instances = createInstances(nodesCount, this)

        instances.forEach { (protocol, _) ->
            assert(instances.map { (_, runner) -> protocol.ping(runner.bindAddress) }.all { it })
        }

        instances.forEach { it.second.close() }
    }

    @Test
    fun `peers are able to find each other connected 1 by 1 in a ring`() = runBlocking {
        val instances = createInstances(nodesCount, this)

        // ping each other in ring (ping will add node into the address book of other node
        instances.forEachIndexed { idx, (protocol, runner) ->
            val addr = if ((idx + 1) >= nodesCount)
                instances[0].second.bindAddress
            else
                instances[idx + 1].second.bindAddress

            protocol.ping(addr)
        }
        assert(instances.map { (protocol, _) -> protocol.addressBook.getRecords().isNotEmpty() }.all { it })

        // try to find each other
        val found = instances.map { (protocolMe, _) ->
            instances.map { (protocolThey, _) -> protocolMe.findNode(protocolThey.addressBook.getMine().id) }
        }

        val correct = found.map { it.all { it != null } }.all { it }
        assert(correct)

        instances.forEach { it.second.close() }
    }

    @Test
    fun `peers are able to find each other after bootstrap`() = runBlocking {
        val instances = createInstances(nodesCount, this)

        // bootstrap via the 1st node
        instances.forEachIndexed { index, (protocol, _) ->
            if (index != 0) assert(protocol.bootstrap(instances[0].second.bindAddress))
            println("NODE $index BOOTSTRAPPED")
        }

        // try to find each other
        val found = instances.mapIndexed { idx, (protocolMe, _) ->
            val foundAll = instances.map { (protocolThey, _) -> protocolMe.findNode(protocolThey.addressBook.getMine().id) }
            println("NODE $idx FOUND ALL OTHER NODES")
            foundAll
        }

        val correct = found.map { it.all { it != null } }.all { it }
        assert(correct)

        instances.forEach { it.second.close() }
    }

    suspend fun createInstances(count: Int, scope: CoroutineScope) = (0 until count).map {
        val addr = InetSocketAddress("localhost",1337 + it)
        val kadId = ByteArray(256).let {
            rng.nextBytes(it)
            BigInteger(1, it)
        }
        val kAddr = KAddress(kadId, addr)
        val addressBook = InMemoryBinaryTrieAddressBook(kAddr, k)

        val runner = ProtocolRunner(addr)
        val protocol = KademliaRoutingProtocol(addressBook)

        runner.registerProtocol(protocol)

        scope.launch(Dispatchers.IO) { runner.run() }

        Pair(protocol, runner)
    }
}

fun randomBigInt(size: Int): BigInteger {
    val bytes = ByteArray(size)
    Random().nextBytes(bytes)

    return BigInteger(1, bytes)
}