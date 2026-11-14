import os
solution = r'F:/sde-assignment-junior-2026/solution'
expected = [
    'SUBMISSION.md',
    'AI_USAGE.md',
    'assignment-a/pipeline.py',
    'assignment-a/database.py',
    'assignment-a/__main__.py',
    'assignment-b/dispatcher.py',
    'assignment-b/__main__.py',
]
for f in expected:
    path = os.path.join(solution, f)
    exists = os.path.exists(path)
    status = 'OK' if exists else 'MISSING'
    print(f'  {status}: {f}')