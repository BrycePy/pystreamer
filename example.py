from pystreamer import Node, Buffer, Destination, start_pystreamer
import random
import time

def rand4():
    print("rand4")
    return tuple(random.randint(0, 100) for _ in range(4))

def add(a, b):
    print("add")
    return (a + b,)

def sub(a, b):
    print("sub")
    return (a - b,)

def mul(a, b):
    print("mul")
    time.sleep(1)
    return (a * b,)

count = [0]
start_time = time.time()
def summary(rand4_0, rand4_1, rand4_2, rand4_3, add, sub, mul):
    print("summary")
    assert add == rand4_0 + rand4_1
    assert sub == rand4_2 - rand4_3
    assert mul == (rand4_0 + rand4_1) * (rand4_2 - rand4_3)
    count[0] += 1
    if count[0] % 100 == 0:
        time_elapsed = time.time() - start_time
        rate = count[0] / time_elapsed
        print(f"Rate: {rate:.2f} results per second")
    

origin = Node(func_output_size=4)
origin.set_func(rand4)

node_1_1 = Node(func_output_size=1)
node_1_1_buffer1 = Buffer(10, "rand4[0]")
node_1_1_buffer2 = Buffer(10, "rand4[1]")
node_1_1.add_buffer(node_1_1_buffer1)
node_1_1.add_buffer(node_1_1_buffer2)
node_1_1.set_func(add)

node_1_2 = Node(func_output_size=1)
node_1_2_buffer1 = Buffer(10, "rand4[2]")
node_1_2_buffer2 = Buffer(10, "rand4[3]")
node_1_2.add_buffer(node_1_2_buffer1)
node_1_2.add_buffer(node_1_2_buffer2)
node_1_2.set_func(sub)

node_2 = Node(func_output_size=1)
node_2_buffer1 = Buffer(10, "add(rand4[0], rand4[1])")
node_2_buffer2 = Buffer(10, "sub(rand4[2], rand4[3])")
node_2.add_buffer(node_2_buffer1)
node_2.add_buffer(node_2_buffer2)
node_2.set_func(mul)

node_3 = Node(func_output_size=0)
node_3_buffer1 = Buffer(10, "rand4[0]")
node_3_buffer2 = Buffer(10, "rand4[1]")
node_3_buffer3 = Buffer(10, "rand4[2]")
node_3_buffer4 = Buffer(10, "rand4[3]")
node_3_buffer5 = Buffer(10, "add result")
node_3_buffer6 = Buffer(10, "sub result")
node_3_buffer7 = Buffer(10, "mul result")
node_3.add_buffer(node_3_buffer1)
node_3.add_buffer(node_3_buffer2)
node_3.add_buffer(node_3_buffer3)
node_3.add_buffer(node_3_buffer4)
node_3.add_buffer(node_3_buffer5)
node_3.add_buffer(node_3_buffer6)
node_3.add_buffer(node_3_buffer7)
node_3.set_func(summary)

origin.add_destination(Destination(node_1_1, node_1_1_buffer1, 0))
origin.add_destination(Destination(node_1_1, node_1_1_buffer2, 1))
origin.add_destination(Destination(node_1_2, node_1_2_buffer1, 2))
origin.add_destination(Destination(node_1_2, node_1_2_buffer2, 3))

node_1_1.add_destination(Destination(node_2, node_2_buffer1, 0))
node_1_2.add_destination(Destination(node_2, node_2_buffer2, 0))

origin.add_destination(Destination(node_3, node_3_buffer1, 0))
origin.add_destination(Destination(node_3, node_3_buffer2, 1))
origin.add_destination(Destination(node_3, node_3_buffer3, 2))
origin.add_destination(Destination(node_3, node_3_buffer4, 3))

node_1_1.add_destination(Destination(node_3, node_3_buffer5, 0))
node_1_2.add_destination(Destination(node_3, node_3_buffer6, 0))
node_2.add_destination(Destination(node_3, node_3_buffer7, 0))

start_pystreamer()