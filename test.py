from cassandra.cluster import Cluster, NoHostAvailable

cluster = Cluster()
session = cluster.connect()