import argparse
import json
import random
import socket
import time
from typing import Any, Dict, List, Optional, Tuple

# Note: In this starter code, we annotate types where
# appropriate. While it is optional, both in python and for this
# course, we recommend it since it makes programming easier.

# The maximum size of the data contained within one packet
payload_size = 1200
# The maximum size of a packet including all the JSON formatting
packet_size = 1500

class Receiver:
    def __init__(self):
        # TODO: Initialize any variables you want here, like the receive
        # buffer, initial congestion window and initial values for the timeout
        # values
        self.buf={}
        self.cur_low = 0
        self.ranges = []

    def data_packet(self, seq_range: Tuple[int, int], data: str) -> Tuple[List[Tuple[int, int]], str]:
        '''This function is called whenever a data packet is
        received. `seq_range` is the range of sequence numbers
        received: It contains two numbers: the starting sequence
        number (inclusive) and ending sequence number (exclusive) of
        the data received. `data` is a binary string of length
        `seq_range[1] - seq_range[0]` representing the data.

        It should output the list of sequence number ranges to
        acknowledge and any data that is ready to be sent to the
        application. Note, data must be sent to the application
        _reliably_ and _in order_ of the sequence numbers. This means
        that if bytes in sequence numbers 0-10000 and 11000-15000 have
        been received, only 0-10000 must be sent to the application,
        since if we send the latter bytes, we will not be able to send
        bytes 10000-11000 in order when they arrive. The transport
        layer must hide hide all packet reordering and loss.

        The ultimate behavior of the program should be that the data
        sent by the sender should be stored exactly in the same order
        at the receiver in a file in the same directory. No gaps, no
        reordering. You may assume that our test cases only ever send
        printable ASCII characters (letters, numbers, punctuation,
        newline etc), so that terminal output can be used to debug the
        program.

        '''

        # TODO
        self.ranges.append(seq_range)
        self.buf.update({seq_range:data})

        self.combine_intervals()
        app_data = self.find_data()
        #self.clean_buffer() # I dont know if we need this
        # data to return is everything from cur_low to end of that range
        return (self.ranges, app_data)

    def finish(self):
        '''Called when the sender sends the `fin` packet. You don't need to do
        anything in particular here. You can use it to check that all
        data has already been sent to the application at this
        point. If not, there is a bug in the code. A real transport
        stack will deallocate the receive buffer. Note, this may not
        be called if the fin packet from the sender is locked. You can
        read up on "TCP connection termination" to know more about how
        TCP handles this.

        '''

        # TODO
        print('finished')
        pass

    # Combine intervals for ranges
    def combine_intervals(self):
        combined = []
        self.ranges.sort(key=lambda x: x[0])
        for i in range(len(self.ranges)):
            start = self.ranges[i][0]
            end = self.ranges[i][1]

            if combined and combined[-1][1]>=end:
                continue

            for j in range(i+1,len(self.ranges)):
                if self.ranges[j][0]==end:
                    end = self.ranges[j][1]

            combined.append((start,end))
        self.ranges=combined

    def clean_buffer(self):
        for key in self.buf:
            if key[0]<self.cur_low:
                del self.buf[key]
        
    # find interval in buff from cur_low to end of that interval
    def find_data(self):
        base = self.cur_low
        #find interval with base
        cur_range = (0,0)
        for r in self.ranges:
            if r[0]<=base and r[1]>=base:
                cur_range = (base, r[1])
                self.cur_low = r[1]
                break

        data = ''
        for key in sorted(self.buf):
            if key[0] >= cur_range[0] and key[0] <= cur_range[1]:
                data+=self.buf[key]
        return data

class Sender:
    def __init__(self, data_len: int):
        '''`data_len` is the length of the data we want to send. A real
        transport will not force the application to pre-commit to the
        length of data, but we are ok with it.

        '''
        # TODO: Initialize any variables you want here, for instance a
        # data structure to keep track of which packets have been
        # sent, acknowledged, detected to be lost or retransmitted
        self.data_len = data_len
        self.num_acked = 0
        self.num_sent = 0
        self.status = {} #-1 not sent, 1 is sent but not acked, 2 is sent and acked, 3 is dropped
        self.packet_ids = {}
        self.dupacks = {}

        self.cwnd = packet_size
        self.RTT_AVG = None
        self.RTT_VAR = None
        self.RTO = 10  # Initial RTO value in seconds
        self.alpha = 1/64  # EWMA smoothing factor
        self.beta = 1/4    # For RTT variance estimation
        self.send_times = {}  # Maps packet_id to send timestamp

        for i in range((data_len//payload_size)+1):
            start = i*payload_size
            end = start+payload_size
            if end > data_len:
                end = data_len
            self.status.update({(start,end):-1})
            self.dupacks.update({(start,end):0})

    def timeout(self):
        '''Called when the sender times out.'''
        # TODO: In addition to what you did in assignment 1, set cwnd to 1
        # packet
        for r in self.status:
            if self.status[r]==1 or self.status[r]==3:
                # self.num_acked=0
                # self.num_sent=0
                self.status[r] = 3
                self.dupacks[r]=0
        # Perform multiplicative decrease
        self.cwnd = max(self.cwnd / 2, packet_size)

    def ack_packet(self, sacks: List[Tuple[int, int]], packet_id: int) -> int:
        '''Called every time we get an acknowledgment. The argument is a list
        of ranges of bytes that have been ACKed. Returns the number of
        payload bytes new that are no longer in flight, either because
        the packet has been acked (measured by the unique ID) or it
        has been assumed to be lost because of dupACKs. Note, this
        number is incremental. For example, if one 100-byte packet is
        ACKed and another 500-byte is assumed lost, we will return
        600, even if 1000s of bytes have been ACKed before this.

        '''

        # TODO
        # deal with acknowledgments
        total_to_ret = 0
        acked_range = self.packet_ids[packet_id]
        if self.status[acked_range]!=1:
            return 0
        self.status[acked_range]=2
        self.num_acked+=1
        total_to_ret+=min(acked_range[1],self.data_len)-acked_range[0]
        print(f"TOTAL_TO_RET:{total_to_ret}")
        sacks.append((0,0))
        sacks.append(((self.data_len//payload_size)*payload_size, self.data_len))

        #calculate RTT
        if packet_id in self.send_times:
            time_delta = time.time() - self.send_times.pop(packet_id)
            # Update RTT estimates using EWMA
            if self.RTT_AVG is None:
                self.RTT_AVG = time_delta
                self.RTT_VAR = time_delta / 2
            else:
                self.RTT_VAR = (1 - self.beta) * self.RTT_VAR + self.beta * abs(time_delta - self.RTT_AVG)
                self.RTT_AVG = (1 - self.alpha) * self.RTT_AVG + self.alpha * time_delta
            # Update RTO
            self.RTO = self.RTT_AVG + 4 * self.RTT_VAR
            # Ensure minimum RTO
            self.RTO = max(self.RTO, 0.1)
        # Perform additive increase
        self.cwnd += (packet_size * packet_size) / self.cwnd

        # deal with dropped from dupeacks
        #print(f"status: {self.status}")
        #print(self.dupacks)
        for i in range(len(sacks)-1):
            prevEnd, curStart = sacks[i][1], sacks[i+1][0]
            while prevEnd < curStart:
                cur_range = (prevEnd, prevEnd+payload_size)
                if self.status[cur_range]==1:
                    self.dupacks.update({cur_range: self.dupacks[cur_range]+1})
                    if self.dupacks[cur_range]==30:
                        self.dupacks[cur_range]=0
                        self.status[cur_range]=3
                        total_to_ret+=min(cur_range[1],self.data_len)-cur_range[0]
                prevEnd+=payload_size

        # deal with dropped from RTO
        current_time = time.time()
        for packet_id, send_time in list(self.send_times.items()):
            seq_range = self.packet_ids[packet_id]
            if self.status[cur_range]==1 and current_time - send_time >= self.RTO:
                self.dupacks[cur_range]=0
                self.status[cur_range]=3
                total_to_ret+=min(cur_range[1],self.data_len)-cur_range[0]
                del self.send_times[packet_id]
        return total_to_ret
            
        # determine dropped?

    def send(self, packet_id: int) -> Optional[Tuple[int, int]]:
        '''Called just before we are going to send a data packet. Should
        return the range of sequence numbers we should send. If there
        are no more bytes to send, returns a zero range (i.e. the two
        elements of the tuple are equal). Return None if there are no
        more bytes to send, and _all_ bytes have been
        acknowledged. Note: The range should not be larger than
        `payload_size` or contain any bytes that have already been
        acknowledged

        '''

        # TODO
        print("Sent vs Acked")
        print(self.num_sent)
        print(self.num_acked)
        
        if self.num_sent==self.data_len//payload_size+1 and self.num_acked==self.num_sent:
            return None
        # if self.num_sent==self.data_len//payload_size+1:
        #     return (0,0)
        range_to_send = (0,0)
        #print(self.status)
        for r in sorted(self.status):
            if self.status[r] == 3: #dropped
                range_to_send = r
                self.status.update({r:1})
                break
            if self.status[r] == -1: #not sent 
                range_to_send = r
                self.status.update({r:1})
                self.num_sent+=1
                break
            
        #print(range_to_send)
        self.packet_ids.update({packet_id:range_to_send})
        self.send_times[packet_id] = time.time()
        return range_to_send

    def get_cwnd(self) -> int:
        return self.cwnd

    def get_rto(self) -> float:
        return self.RTO

def start_receiver(ip: str, port: int):
    '''Starts a receiver thread. For each source address, we start a new
    `Receiver` class. When a `fin` packet is received, we call the
    `finish` function of that class.

    We start listening on the given IP address and port. By setting
    the IP address to be `0.0.0.0`, you can make it listen on all
    available interfaces. A network interface is typically a device
    connected to a computer that interfaces with the physical world to
    send/receive packets. The WiFi and ethernet cards on personal
    computers are examples of physical interfaces.

    Sometimes, when you start listening on a port and the program
    terminates incorrectly, it might not release the port
    immediately. It might take some time for the port to become
    available again, and you might get an error message saying that it
    could not bind to the desired port. In this case, just pick a
    different port. The old port will become available soon. Also,
    picking a port number below 1024 usually requires special
    permission from the OS. Pick a larger number. Numbers in the
    8000-9000 range are conventional.

    Virtual interfaces also exist. The most common one is `localhost',
    which has the default IP address of `127.0.0.1` (a universal
    constant across most machines). The Mahimahi network emulator also
    creates virtual interfaces that behave like real interfaces, but
    really only emulate a network link in software that shuttles
    packets between different virtual interfaces.

    '''

    receivers: Dict[str, Receiver] = {}

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_socket:
        server_socket.bind((ip, port))
        received_data = ''
        while True:
            data, addr = server_socket.recvfrom(packet_size)
            if addr not in receivers:
                receivers[addr] = Receiver()

            received = json.loads(data.decode())
            if received["type"] == "data":
                # Format check. Real code will have much more
                # carefully designed checks to defend against
                # attacks. Can you think of ways to exploit this
                # transport layer and cause problems at the receiver?
                # This is just for fun. It is not required as part of
                # the assignment.
                assert type(received["seq"]) is list
                assert type(received["seq"][0]) is int and type(received["seq"][1]) is int
                assert type(received["payload"]) is str
                assert len(received["payload"]) <= payload_size

                # Deserialize the packet. Real transport layers use
                # more efficient and standardized ways of packing the
                # data. One option is to use protobufs (look it up)
                # instead of json. Protobufs can automatically design
                # a byte structure given the data structure. However,
                # for an internet standard, we usually want something
                # more custom and hand-designed.
                sacks, app_data = receivers[addr].data_packet(tuple(received["seq"]), received["payload"])
                received_data += app_data
                # Note: we immediately write the data to file
                #receivers[addr][1].write(app_data)

                # Send the ACK
                server_socket.sendto(json.dumps({"type": "ack", "sacks": sacks, "id": received["id"]}).encode(), addr)


            elif received["type"] == "fin":
                if received_data:
                    print("received data (summary): ", received_data[:100], "...", len(received_data))
                    # print("received file is saved into: ", receivers[addr][1].name)
                    server_socket.sendto(json.dumps({"type": "fin"}).encode(), addr)

                    with open("final_output.txt", 'w') as f:
                        f.write(received_data)
                    received_data = ''
                receivers[addr].finish()
                del receivers[addr]

            else:
                assert False

def start_sender(ip: str, port: int, data: str, recv_window: int, simloss: float):
    sender = Sender(len(data))

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client_socket:
        # So we can receive messages
        client_socket.connect((ip, port))
        # When waiting for packets when we call receivefrom, we
        # shouldn't wait more than 500ms

        # Number of bytes that we think are inflight. We are only
        # including payload bytes here, which is different from how
        # TCP does things
        inflight = 0
        packet_id  = 0
        wait = False

        while True:
            # Get the congestion condow
            cwnd = sender.get_cwnd()

            # Do we have enough room in recv_window to send an entire
            # packet?
            if inflight + packet_size <= min(recv_window, cwnd) and not wait:
                seq = sender.send(packet_id)
                if seq is None:
                    # We are done sending
                    client_socket.send('{"type": "fin"}'.encode())
                    break
                elif seq[1] == seq[0]:
                    # No more packets to send until loss happens. Wait
                    wait = True
                    continue

                assert seq[1] - seq[0] <= payload_size
                assert seq[1] <= len(data)

                # Simulate random loss before sending packets
                if random.random() < simloss:
                    pass
                else:
                    # Send the packet
                    client_socket.send(
                        json.dumps(
                            {"type": "data", "seq": seq, "id": packet_id, "payload": data[seq[0]:seq[1]]}
                        ).encode())

                inflight += seq[1] - seq[0]
                packet_id += 1

            else:
                wait = False
                # Wait for ACKs
                try:
                    rto = sender.get_rto()
                    client_socket.settimeout(rto)
                    received_bytes = client_socket.recv(packet_size)
                    received = json.loads(received_bytes.decode())
                    assert received["type"] == "ack"

                    if random.random() < simloss:
                        continue

                    inflight -= sender.ack_packet(received["sacks"], received["id"])
                    assert inflight >= 0
                except socket.timeout:
                    inflight = 0
                    print("Timeout")
                    sender.timeout()


def main():
    parser = argparse.ArgumentParser(description="Transport assignment")
    parser.add_argument("role", choices=["sender", "receiver"], help="Role to play: 'sender' or 'receiver'")
    parser.add_argument("--ip", type=str, required=True, help="IP address to bind/connect to")
    parser.add_argument("--port", type=int, required=True, help="Port number to bind/connect to")
    parser.add_argument("--sendfile", type=str, required=False, help="If role=sender, the file that contains data to send")
    parser.add_argument("--recv_window", type=int, default=15000000, help="Receive window size in bytes")
    parser.add_argument("--simloss", type=float, default=0.0, help="Simulate packet loss. Provide the fraction of packets (0-1) that should be randomly dropped")

    args = parser.parse_args()

    if args.role == "receiver":
        start_receiver(args.ip, args.port)
    else:
        if args.sendfile is None:
            print("No file to send")
            return

        with open(args.sendfile, 'r') as f:
            data = f.read()
            start_sender(args.ip, args.port, data, args.recv_window, args.simloss)

if __name__ == "__main__":
    main()
