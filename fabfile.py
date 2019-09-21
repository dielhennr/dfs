'''

Usage:

First ssh into a lab machine (doesn't matter which one). Create a virtualenv by running: 'virtualenv venv'
Run: 'source venv/bin/activate'
You will now be operating in a virtual env
Install fabric with: pip install 'fabric>=1.12.0,<1.13.0'

Go into the directory with fabfile.py
Run: fab -l
You should see 'start' as a fabric task
Run: fab start
Boom
Make sure you have Controller started before you run this, and also make sure you change the path in the fabfile to the correct location of the P1-ryan folder in your home directory
'''
from fabric.api import task, parallel, run, execute

hosts = ['orion02', 'orion03', 'orion04', 'orion05', 'orion06', 'orion07',
         'orion08', 'orion09', 'orion10', 'orion11', 'orion12']


@task
def start():
    execute(start_nodes, hosts=hosts)


@parallel
def start_nodes():
    cmd = 'java -cp /home4/dhutchinson/P1-ryan/target/dfs-1.0.jar edu.usfca.cs.dfs.StorageNode'
    run(cmd)

