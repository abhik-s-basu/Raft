dump_files = ['./logs_node_0/dump.txt', './logs_node_1/dump.txt', './logs_node_2/dump.txt', './logs_node_3/dump.txt', './logs_node_4/dump.txt']

log_files = ['./logs_node_0/logs.txt', './logs_node_1/logs.txt', './logs_node_2/logs.txt', './logs_node_3/logs.txt', './logs_node_4/logs.txt',]

meta_files = ['./logs_node_0/metadata.txt', './logs_node_1/metadata.txt', './logs_node_2/metadata.txt', './logs_node_3/metadata.txt', './logs_node_4/metadata.txt']

for filename in dump_files:
    with open(filename, 'w') as file:
        pass

for filename in log_files:
    with open(filename, 'w') as file:
        pass

for filename in meta_files:
    with open(filename, 'w') as file:
        pass

print("All files have been cleared.")
