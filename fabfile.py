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

