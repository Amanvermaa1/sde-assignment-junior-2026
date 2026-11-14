"""Run the Assignment B dispatcher."""
import sys
import os

# Add the script's directory to path (assignment-b)
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)

# Now dispatcher is in the same directory
from dispatcher import main

main()