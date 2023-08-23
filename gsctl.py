import sys
import os

script_dir = os.path.abspath("python/graphscope/gsctl")
#Set priorities
sys.path.insert(0,script_dir)

import gsctl

gsctl.cli()

