filenames = ['./logs_node_0/logs.txt', './logs_node_1/logs.txt', './logs_node_2/logs.txt', './logs_node_3/logs.txt', './logs_node_4/logs.txt']

for filename in filenames:
    with open(filename, 'w') as file:
        pass

print("All files have been cleared.")
