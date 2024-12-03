import cv2
import time
import numpy as np

cap = cv2.VideoCapture("12min.mp4")
count = 0
start = time.time()

while True:
    ret, frame = cap.read()
    if not ret: break
    count += 1
    
    # frame_transform = frame.astype(np.float32) / 255.0
    blur = cv2.GaussianBlur(frame, (31, 31), 0)
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    edges = cv2.Canny(gray, 100, 200)
    
    
    if count % 100 == 0:
        fps = count / (time.time() - start)
        print(f"Processed, {count}, {fps:.2f} fps")
    
    