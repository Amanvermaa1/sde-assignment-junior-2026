import os
r = 'test_data'
for root, dirs, files in os.walk(r):
    for f in files:
        print(os.path.join(root, f))