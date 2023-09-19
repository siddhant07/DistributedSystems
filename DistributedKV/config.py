#Common configs for all implementations
replica_cnt = 3
HOST = "127.0.0.1"
kv_store_name = "kv_store_"
TYPE = 'utf-8'
broadcast_port = 5100
primary_port = 5200

#Configs for Sequential Consistency
sequential_port = 5300
Sequential_dir = "Sequential/"
sequential_prim_port = 5400

#Configs for Linearizable
linearizable_port = 5500
linearizable_dir = "Linearizable/"
linearizable_prim_port = 5600

#Configs for Eventual Consistency
Eventual_dir = "Eventual/"
eventual_port = 5700

#Configs for Causal Consistency
causal_port = 6900
causal_dir = "Causal/"