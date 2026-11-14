import os
os.chdir(r'F:\sde-assignment-junior-2026\solution\assignment-a')
from pathlib import Path
p = Path('test_data/drop_1')
print('exists:', p.exists())
print('leads exists:', (p / 'leads').exists())
print('events exists:', (p / 'events').exists())