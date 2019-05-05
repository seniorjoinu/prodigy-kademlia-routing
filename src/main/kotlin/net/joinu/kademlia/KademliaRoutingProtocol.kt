package net.joinu.kademlia

import net.joinu.kademlia.addressbook.AddressBook
import net.joinu.kademlia.addressbook.getMyClusterExceptMe
import net.joinu.prodigy.AbstractProtocol
import net.joinu.prodigy.protocol
import java.net.InetSocketAddress


const val KAD_TOPIC = "_KAD"
object KadMsgTypes {
    const val PING = "PING"
    const val FIND_NODE = "FIND_NODE"
    const val GREET = "GREET"
    const val BYE = "BYE"
}

class KademliaRoutingProtocol(val addressBook: AddressBook) : AbstractProtocol() {

    override val protocol = protocol(KAD_TOPIC) {
        on(KadMsgTypes.PING) {
            val senderId = request.getPayloadAs<KadId>()
            val sender = request.sender

            addressBook.addRecord(KAddress(senderId, sender))

            request.respond(addressBook.getMine().id)
        }

        on(KadMsgTypes.GREET) {
            val senderId = request.getPayloadAs<KadId>()
            val sender = request.sender

            addressBook.addRecord(KAddress(senderId, sender))
        }

        on(KadMsgTypes.BYE) {
            val senderId = request.getPayloadAs<KadId>()

            addressBook.removeRecord(senderId)
        }

        on(KadMsgTypes.FIND_NODE) {
            val payload = request.getPayloadAs<FindNodeRequest>()
            val sender = request.sender

            val peerExact = if (addressBook.getMine().id == payload.findId)
                addressBook.getMine()
            else
                addressBook.getRecordById(payload.findId)

            val peersToAsk = addressBook.getCluster(payload.findId).peers.sortedBy { it.id.xor(payload.findId) }

            val result = if (peerExact != null)
                FindNodeResponse(peerExact, null)
            else
                FindNodeResponse(null, peersToAsk)

            addressBook.addRecord(KAddress(payload.senderId, sender))

            request.respond(result)
        }
    }

    private val asked = hashMapOf<KadId, MutableSet<KAddress>>()

    suspend fun bootstrap(bootstrapPeer: InetSocketAddress): Boolean {
        if (!ping(bootstrapPeer)) return false

        findNode(addressBook.getMine().id)

        val myCluster = addressBook.getCluster(addressBook.getMine().id)
        myCluster.peers.forEach { greet(it.address) }

        return true
    }

    suspend fun byeMyCluster() {
        addressBook.getMyClusterExceptMe().peers.forEach { bye(it.address) }
    }

    suspend fun ping(peer: InetSocketAddress): Boolean {
        val peerId = sendAndReceive<KadId>(KAD_TOPIC, KadMsgTypes.PING, peer, addressBook.getMine().id)
        addressBook.addRecord(KAddress(peerId, peer))

        return true
    }

    suspend fun greet(peer: InetSocketAddress) {
        send(KAD_TOPIC, KadMsgTypes.GREET, peer, addressBook.getMine().id)
    }

    suspend fun bye(peer: InetSocketAddress) {
        send(KAD_TOPIC, KadMsgTypes.BYE, peer, addressBook.getMine().id)

        val peerAddr = addressBook.getRecords().find { it.address == peer } ?: return
        addressBook.removeRecord(peerAddr.id)
    }

    suspend fun findNode(findId: KadId): KAddress? {
        // if already have him in address book - return
        val peerFromAddressBook = addressBook.getRecordById(findId)
        if (peerFromAddressBook != null) return peerFromAddressBook

        // initialize list of asked peers if it is uninitialized
        if (!asked.containsKey(findId)) asked[findId] = mutableSetOf()

        // first of all search the closest known peers (from cluster)
        while (true) {
            val cluster = addressBook.getCluster(findId)
            val closestPeer = cluster.peers
                .sortedBy { it.id.xor(findId) }
                .firstOrNull { !asked[findId]!!.contains(it) } ?: break

            val result = sendFindNodeAndProcessResult(closestPeer, findId)
            if (result != null) return result
        }

        // if that didn't work - search other peers which you know
        while (true) {
            val peers = addressBook.getRecords()
            val closestPeer = peers
                .sortedBy { it.id.xor(findId) }
                .firstOrNull { !asked[findId]!!.contains(it) }

            // if we can't find a single node who knows about [findId] - return nothing
            if (closestPeer == null) {
                asked.remove(findId)
                return null
            }

            val result = sendFindNodeAndProcessResult(closestPeer, findId)
            if (result != null) return result
        }
    }

    private suspend fun sendFindNodeAndProcessResult(peer: KAddress, findId: KadId): KAddress? {
        val response = sendFindNode(peer.address, findId)

        if (response.peerExact != null) {
            addressBook.addRecord(response.peerExact)
            asked.remove(findId)

            return response.peerExact
        }

        asked[findId]!!.add(peer)

        response.peersToAsk!!.forEach { addressBook.addRecord(it) }

        return null
    }

    private suspend fun sendFindNode(peer: InetSocketAddress, findId: KadId): FindNodeResponse {
        val payload = FindNodeRequest(addressBook.getMine().id, findId)

        return sendAndReceive(KAD_TOPIC, KadMsgTypes.FIND_NODE, peer, payload)
    }
}