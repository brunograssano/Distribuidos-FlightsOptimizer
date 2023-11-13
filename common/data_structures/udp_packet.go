package data_structures

type UDPPacket struct {
	PacketType    uint8
	CoordinatorID uint8
}

const SizeUdpPacket = 2
const ACK = 0
const Election = 1
const Coordinator = 2
