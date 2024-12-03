from inspect import signature
from mpi4py import MPI
from threading import Thread, Lock
import time
import numpy as np

buffer_tag_counter = 1000000000
node_rank_counter = 0
global_nodes = []

def get_buffer_tag():
    global buffer_tag_counter
    output = buffer_tag_counter
    buffer_tag_counter += 1
    return output

def get_node_rank():
    global node_rank_counter
    output = node_rank_counter
    node_rank_counter += 1
    return output

class NodeFunction:
    def __init__(self):
        pass
    
    def setup(self):
        pass
    
    def run(self):
        pass

class Buffer:
    def __init__(self, size, name=""):
        self.size = size
        self.name = name
        self.data = []
        
        self.comm_tag = get_buffer_tag()
        self.comm_rank = None
        self.comm_pending_ack = []
        
        self.message_counter = 0
        
        self.lock_pending_ack = Lock()

    def assign_comm_rank(self, rank):
        if self.comm_rank is not None:
            raise ValueError("Comm rank already assigned")
        self.comm_rank = rank

    def is_empty(self):
        return len(self.data) == 0

    def send(self, data):
        comm = MPI.COMM_WORLD
        comm.send(data, dest=self.comm_rank, tag=self.comm_tag)
        comm.recv(source=self.comm_rank, tag=self.comm_tag)

    def isend(self, data):
        comm = MPI.COMM_WORLD
        self.data.append(data)
        comm.isend(data, dest=self.comm_rank, tag=self.comm_tag)

    def wait_ack(self):
        comm = MPI.COMM_WORLD
        comm.recv(source=self.comm_rank, tag=self.comm_tag)
        self.data.pop(0)

    def get(self):
        return self.data.pop(0)

    def _receive(self):
        comm = MPI.COMM_WORLD
        status = MPI.Status()
        data = comm.recv(tag=self.comm_tag, status=status)
        if len(self.data) >= self.size:
            self._pending_ack(status.Get_source(), self.comm_tag)
        else:
            self._acknowledge(status.Get_source(), self.comm_tag)
        self.data.append(data)

    def _pending_ack(self, rank, tag):
        with self.lock_pending_ack:
            self.comm_pending_ack.append((rank, tag))

    def check_pending_ack(self):
        with self.lock_pending_ack:
            if len(self.comm_pending_ack) == 0:
                return
            self._acknowledge(*self.comm_pending_ack.pop(0))

    def _acknowledge(self, rank, tag):
        comm = MPI.COMM_WORLD
        comm.send(0, dest=rank, tag=tag)

    def _listen(self):
        while True:
            self._receive()
            self.message_counter+=1
            # if self.message_counter % 100 == 0:
                # print(f"Buffer {self.name} received {self.message_counter} messages. Current buffer size: {len(self.data)}")

    def listen(self):
        thread = Thread(target=self._listen)
        thread.start()

class BBuffer(Buffer):
    def __init__(self, size, name="", shape=None, dtype=None):
        super().__init__(size, name)
        self.buffer = np.zeros(shape, dtype=dtype)

    def _receive(self):
        comm = MPI.COMM_WORLD
        status = MPI.Status()
        comm.Recv(self.buffer, tag=self.comm_tag, status=status)
        if len(self.data) >= self.size:
            self._pending_ack(status.Get_source(), self.comm_tag)
        else:
            self._acknowledge(status.Get_source(), self.comm_tag)
        self.data.append(self.buffer.copy())

    def send(self, data):
        comm = MPI.COMM_WORLD
        self.data.append(data)
        comm.Send(data, dest=self.comm_rank, tag=self.comm_tag)
        self.data.pop(0)
        comm.recv(source=self.comm_rank, tag=self.comm_tag)

    def isend(self, data):
        comm = MPI.COMM_WORLD
        self.data.append(data)
        comm.Isend(data, dest=self.comm_rank, tag=self.comm_tag)
        
    def wait_ack(self):
        comm = MPI.COMM_WORLD
        comm.recv(source=self.comm_rank, tag=self.comm_tag)
        self.data.pop(0)
        

class Destination:
    def __init__(self, node, buffer, output_index):
        self.node = node
        self.buffer = buffer
        self.output_index = output_index

class Node:
    def __init__(self, func_output_size):
        self.sources = []
        self.destinations = []
        self.src_buffers = []
        self.func = None
        self.nodefunc = None
        self.func_output_size = func_output_size
        self.rank = get_node_rank()
        self.latency = 0.0001
        global_nodes.append(self)
        
    def add_destination(self, dst):
        if dst.output_index < 0:
            raise ValueError("Output index must be greater than 0")
        if dst.output_index >= self.func_output_size:
            raise ValueError(f"Output index must be less than {self.func_output_size}")
        self.destinations.append(dst)
        self.sources.append(dst.node)
        
        if self.rank == MPI.COMM_WORLD.Get_rank():
            print(f"Fowarding output_index {dst.output_index} from {self.rank} to {dst.node.rank}@{dst.buffer.comm_tag}")

    def add_buffer(self, buffer):
        self.src_buffers.append(buffer)
        buffer.assign_comm_rank(self.rank)

    def set_func(self, func):
        if self.func is not None:
            raise ValueError("Function already set")
        
        if isinstance(func, NodeFunction):
            func_params = len(signature(func.run).parameters)
            self.nodefunc = func
            self.func = func.run
        else:
            func_params = len(signature(func).parameters)
            self.func = func
        src_params = len(self.src_buffers)
        if func_params != src_params:
            raise ValueError(f"Function must have {src_params} parameter")

    def forward_output(self, output):
        for dst in self.destinations:
            dst.buffer.isend(output[dst.output_index])
        for dst in self.destinations:
            dst.buffer.wait_ack()

    def buffer_acknowledge(self):
        for buffer in self.src_buffers:
            buffer.check_pending_ack()

    def list_reachable_node(self):
        nodes = [self]
        queue = [self]
        while queue:
            current_node = queue.pop(0)
            for dst in current_node.destinations:
                if dst.node not in nodes:
                    nodes.append(dst.node)
                    queue.append(dst.node)
        return nodes

    def ready(self):
        for buffer in self.src_buffers:
            if buffer.is_empty():
                return False
        return True

    def run(self):
        time.sleep(self.latency)
        if not self.ready():
            self.latency *= 1.01
            return
        self.latency *= 0.999
        args = [buffer.get() for buffer in self.src_buffers]
        output = self.func(*args)
        self.buffer_acknowledge()
        self.forward_output(output)
        
    def listen(self):
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        if rank != self.rank:
            raise Exception("Node rank must be equal to MPI rank")
    
        if self.nodefunc:
            self.nodefunc.setup()
    
        for buffer in self.src_buffers:
            buffer.listen()
    
        while True:
            self.run()
        
def start_pystreamer():
    if MPI.COMM_WORLD.Get_size() != len(global_nodes):
        raise Exception(f"MPI size must be equal to the number of nodes. Expected {len(global_nodes)}")
    
    for node in global_nodes:
        if node.rank == MPI.COMM_WORLD.Get_rank():
            node.listen() 