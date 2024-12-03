from pystreamer import Node, BBuffer, Buffer, Destination, NodeFunction, start_pystreamer
from mpi4py import MPI
import cv2
import numpy as np
import time
import uuid

class video_reader(NodeFunction):
    def __init__(self, video_path):
        self.video_path = video_path
    
    def setup(self):
        self.cap = cv2.VideoCapture(self.video_path)
    
    def run(self):
        ret, frame = self.cap.read()
        return (frame,)

class video_transform(NodeFunction):
    def run(self, frame):
        frame_transform = cv2.GaussianBlur(frame, (31, 31), 0)
        return (frame_transform,)

class video_edge(NodeFunction):
    def run(self, frame):
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        edges = cv2.Canny(gray, 100, 200)
        dummy_data = {
            "uuid": uuid.uuid4(),
            "n_pixels": np.count_nonzero(edges),
            "random_number": {
                "a": np.random.random((10, 10)).tolist()
            }
        }
        return (edges, dummy_data)

class video_show(NodeFunction):
    def setup(self):
        self.start_time = time.time()
        self.count = 0
    
    def run(self, blur, edge, dummy_data):
        self.count += 1
        rate = self.count / (time.time() - self.start_time)
        if self.count % 100 == 0:
            print(f"Processed, {self.count}, {rate:.2f} fps")
            print(dummy_data["uuid"])

n1 = Node(func_output_size=1)
n1.set_func(video_reader("12min.mp4"))

n2 = Node(func_output_size=1)
n2_buffer1 = BBuffer(10, "frame", shape=(1080, 1920, 3), dtype=np.uint8)
n2.add_buffer(n2_buffer1)
n2.set_func(video_transform())

n3 = Node(func_output_size=2)
n3_buffer1 = BBuffer(10, "frame", shape=(1080, 1920, 3), dtype=np.uint8)
n3.add_buffer(n3_buffer1)
n3.set_func(video_edge())

n4 = Node(func_output_size=1)
n4_buffer1 = BBuffer(10, "blur", shape=(1080, 1920, 3), dtype=np.uint8)
n4_buffer2 = BBuffer(10, "edge", shape=(1080, 1920, 1), dtype=np.uint8)
n4_buffer3 = Buffer(10, "dummy_data")
n4.add_buffer(n4_buffer1)
n4.add_buffer(n4_buffer2)
n4.add_buffer(n4_buffer3)
n4.set_func(video_show())

n1.add_destination(Destination(n2, n2_buffer1, 0))
n1.add_destination(Destination(n3, n3_buffer1, 0))
n2.add_destination(Destination(n4, n4_buffer1, 0))
n3.add_destination(Destination(n4, n4_buffer2, 0))
n3.add_destination(Destination(n4, n4_buffer3, 1))

start_pystreamer()