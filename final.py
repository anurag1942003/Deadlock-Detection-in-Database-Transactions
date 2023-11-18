import mysql.connector
import threading
from time import sleep
import logging
import random
from threading import Semaphore
config = {
    'user': 'root',
    'password': 'nodudeno',
    'host': '127.0.0.1',
    'database': 'Deadlock',
    'raise_on_warnings': True
}

thread_to_transaction_map = {}
account_semaphores = {i: Semaphore() for i in range(1, 21)}

# Set up logging
logging.basicConfig(filename='transactions.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

locks = {i: 'unlocked' for i in range(1, 21)}
deadlock_detected = False
deadlock_resolved = threading.Condition()


def acquire_lock(account_id, thread_name):
    # Attempt to acquire the semaphore for the given account
    if account_semaphores[account_id].acquire(blocking=False):
        locks[account_id] = thread_name  # Track which thread holds the lock
        return True
    else:
        # The account is locked by another thread
        # Update the wait_for_graph to reflect that this thread is waiting for the lock holder
        current_lock_holder = locks.get(account_id)
        if current_lock_holder and current_lock_holder != thread_name:
            wait_for_graph[thread_name].append(current_lock_holder)
        return False


def release_lock(account_id, thread_name):
    # Check if the current thread holds the lock on the account
    if locks.get(account_id) == thread_name:
        # Release the semaphore for the account
        account_semaphores[account_id].release()
        # Update the lock tracking to indicate the account is now unlocked
        locks[account_id] = 'unlocked'


wait_for_graph = {f'thread{i}': [] for i in range(1, 101)}

# DFS search: as we are going in depth of all nodes it's been visited and is waiting for
# the current node to finish, we are gonna use recursion


def has_cycle(node, visited, rec_stack):
    visited[node] = True
    rec_stack[node] = True

    for neighbor in wait_for_graph.get(node, []):  # Use .get to avoid KeyError
        # Use .get to provide a default value of False
        if not visited.get(neighbor, False):
            if has_cycle(neighbor, visited, rec_stack):
                return True
        # Use .get to provide a default value of False
        elif rec_stack.get(neighbor, False):
            return True

    # Used to indicate we finished exploring and we are backtracking and clearing rec_stack
    rec_stack[node] = False
    return False


def detect_cycle():
    global deadlock_detected
    with deadlock_resolved:
        visited = {node: False for node in wait_for_graph}
        # Recursion stack: used to keep track of node we are gonna explore
        rec_stack = {node: False for node in wait_for_graph}

        for node in wait_for_graph:
            if not visited[node]:
                if has_cycle(node, visited, rec_stack):
                    # Before declaring a deadlock, check if it's already been resolved
                    if deadlock_detected:
                        return False
                    return True
    return False


def print_cycle(node, visited, path):
    visited[node] = True
    path.append(node)

    for neighbor in wait_for_graph.get(node, []):
        if not visited.get(neighbor, False):
            cycle_path = print_cycle(neighbor, visited, path)
            if cycle_path:
                return cycle_path
        elif neighbor in path:
            # Cycle detected, return the path with the start node added at the end to complete the cycle
            cycle_start_index = path.index(neighbor)
            cycle_path = path[cycle_start_index:]
            # Append the start node to complete the cycle
            cycle_path.append(neighbor)
            return cycle_path

    path.pop()
    return None


def visualize_deadlock_cycle():
    visited = {node: False for node in wait_for_graph}
    for node in wait_for_graph:
        if not visited[node]:
            cycle_path = print_cycle(node, visited, [])
            if cycle_path:
                cycle_str = " -> ".join(cycle_path)
                logging.info(f"Cycle: {cycle_str}")
                break


def transact(src_id, dest_id, amount, thread_name):
    global thread_to_transaction_map
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    src_lock_acquired = False
    dest_lock_acquired = False
    try:
        # Try to acquire lock for source account
        src_lock_acquired = acquire_lock(src_id, thread_name)
        if not src_lock_acquired:
            logging.info(
                f"Transaction between {src_id} and {dest_id} could not proceed due to locks on account {src_id}.")
            return False

        sleep(3)
        dest_lock_acquired = acquire_lock(dest_id, thread_name)
        if not dest_lock_acquired:
            logging.info(
                f"Transaction between {src_id} and {dest_id} could not proceed due to locks on account {dest_id}.")
            thread_to_transaction_map[thread_name] = {
                'src_id': src_id,
                'dest_id': dest_id,
                'amount': amount,
                'thread_name': thread_name
            }

            # Detect cycle in the wait_for_graph
            if detect_cycle():
                global deadlock_detected
                with deadlock_resolved:
                    if not deadlock_detected:
                        deadlock_detected = True  # Check again with the condition acquired
                        logging.info("Deadlock detected!")
                        visualize_deadlock_cycle()
                        resolve_deadlock(cursor)
                    else:
                        deadlock_resolved.wait()
            if src_lock_acquired:
                release_lock(src_id, thread_name)
            thread_to_transaction_map.clear()
            return False

        # Perform the transaction
        cursor.execute(
            "INSERT INTO Transactions (src_id, dest_id, amount) VALUES (%s, %s, %s)",
            (src_id, dest_id, amount)
        )
        cursor.execute(
            f"UPDATE Accounts SET balance = balance - {amount} WHERE id = {src_id}")
        cursor.execute(
            f"UPDATE Accounts SET balance = balance + {amount} WHERE id = {dest_id}")
        logging.info(
            f"Transaction between {src_id} and {dest_id} for amount {amount} was successful.")
        conn.commit()
    finally:
        if src_lock_acquired:
            release_lock(src_id, thread_name)
        if dest_lock_acquired:
            release_lock(dest_id, thread_name)
        cursor.close()
        conn.close()
    return True


def resolve_deadlock(cursor):
    global deadlock_detected
    with deadlock_resolved:
        logging.info(
            "Resolving deadlock based on account priority and transaction amount.")
        deadlocked_transactions = get_deadlocked_transactions(cursor)
        sorted_transactions = sorted(
            deadlocked_transactions, key=lambda x: (-x['src_level'], -x['amount']))
        # Clear the wait_for_graph
        for key in wait_for_graph:
            wait_for_graph[key] = []
        # Release all locks and corresponding semaphores
        for account_id in locks.keys():
            if locks[account_id] != 'unlocked':
                # Release the semaphore
                account_semaphores[account_id].release()
            locks[account_id] = 'unlocked'
        deadlock_detected = False  # Reset the flag after resolving the deadlock
        deadlock_resolved.notify_all()
        for transaction in sorted_transactions:
            transact(transaction['src_id'], transaction['dest_id'],
                     transaction['amount'], transaction['thread_name'])
        logging.info(
            "Deadlock resolved and transactions reattempted based on priority.")


def get_deadlocked_transactions(cursor):
    deadlocked_threads = set()
    global thread_to_transaction_map
    for thread, waiting_on in wait_for_graph.items():
        if waiting_on:  # If the thread is waiting on another thread
            deadlocked_threads.add(thread)
            deadlocked_threads.update(waiting_on)
    deadlocked_transactions = []
    for thread in deadlocked_threads:
        transaction_details = thread_to_transaction_map[thread]
        src_id = transaction_details['src_id']
        src_level = get_account_level(src_id, cursor)
        transaction_details['src_level'] = src_level
        deadlocked_transactions.append(transaction_details)
    return deadlocked_transactions


def get_account_level(account_id, cursor):
    query = "SELECT level FROM Accounts WHERE id = %s"
    cursor.execute(query, (account_id,))
    result = cursor.fetchone()
    return result[0] if result else None


threads = []
num_transactions = 10

for i in range(1, num_transactions * 2, 2):
    src_id = i
    dest_id = i + 1
    amount = 50
    thread_name = f'thread{i//2 + 1}'

    # Create and start the thread
    thread = threading.Thread(target=transact, args=(
        src_id, dest_id, amount, thread_name))
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()

# Initialize a list to hold the threads
threads = []

deadlock_transactions = [
    {"src": 1, "dest": 2, "amount": 50},
    {"src": 2, "dest": 1, "amount": 50}
]

# Create and start a thread for each transaction
for i, transaction in enumerate(deadlock_transactions):
    thread_name = f'thread{i+1}'  # Create a unique thread name
    thread = threading.Thread(
        target=transact,
        args=(transaction["src"], transaction["dest"],
              transaction["amount"], thread_name)
    )
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()


for i in range(1, 16):
    src_id = random.randint(1, 20)
    dest_id = random.randint(1, 20)
    while dest_id == src_id:
        dest_id = random.randint(1, 20)
    amount = int(random.uniform(10, 100))
    transact(src_id, dest_id, amount,
             f'thread{i+12}')


# Initialize a list to hold the threads
threads = []

deadlock_transactions = [
    {"src": 3, "dest": 4, "amount": 50},
    {"src": 4, "dest": 5, "amount": 50},
    {"src": 5, "dest": 3, "amount": 50}
]

# Create and start a thread for each transaction
for i, transaction in enumerate(deadlock_transactions):
    thread_name = f'thread{i+1}'  # Create a unique thread name
    thread = threading.Thread(
        target=transact,
        args=(transaction["src"], transaction["dest"],
              transaction["amount"], thread_name)
    )
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()


# Initialize a list to hold the threads
threads = []

# Define the transactions
transactions = [
    {"src": 10, "dest": 11, "amount": 50},
    {"src": 12, "dest": 13, "amount": 50},
    {"src": 14, "dest": 15, "amount": 50},
    {"src": 16, "dest": 17, "amount": 50},
    {"src": 19, "dest": 20, "amount": 50},
    {"src": 3, "dest": 18, "amount": 50},
    {"src": 5, "dest": 4, "amount": 50},
    {"src": 6, "dest": 7, "amount": 50},
    {"src": 1, "dest": 2, "amount": 50},
    {"src": 8, "dest": 9, "amount": 50}
]

# Create and start a thread for each transaction
for i, transaction in enumerate(transactions):
    thread_name = f'thread{i+1}'  # Create a unique thread name
    thread = threading.Thread(
        target=transact,
        args=(transaction["src"], transaction["dest"],
              transaction["amount"], thread_name)
    )
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()


# Initialize a list to hold the threads
threads = []


deadlock_transactions = [
    {"src": 6, "dest": 7, "amount": 50},
    {"src": 7, "dest": 8, "amount": 50},
    {"src": 8, "dest": 9, "amount": 50},
    {"src": 9, "dest": 6, "amount": 50}
]

# Create and start a thread for each transaction
for i, transaction in enumerate(deadlock_transactions):
    thread_name = f'thread{i+1}'  # Create a unique thread name
    thread = threading.Thread(
        target=transact,
        args=(transaction["src"], transaction["dest"],
              transaction["amount"], thread_name)
    )
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()


for i in range(1, 40):
    src_id = random.randint(1, 20)
    dest_id = random.randint(1, 20)
    while dest_id == src_id:
        dest_id = random.randint(1, 20)
    amount = int(random.uniform(10, 100))
    transact(src_id, dest_id, amount,
             f'thread{i+12}')  # thread 26


# Initialize a list to hold the threads
threads = []


deadlock_transactions = [
    {"src": 1, "dest": 2, "amount": 50},
    {"src": 2, "dest": 1, "amount": 50}
]

# Create and start a thread for each transaction
for i, transaction in enumerate(deadlock_transactions):
    thread_name = f'thread{i+1}'  # Create a unique thread name
    thread = threading.Thread(
        target=transact,
        args=(transaction["src"], transaction["dest"],
              transaction["amount"], thread_name)
    )
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()


# Initialize a list to hold the threads
threads = []

# Define the transactions
transactions = [
    {"src": 16, "dest": 17, "amount": 50},
    {"src": 19, "dest": 20, "amount": 50},
    {"src": 3, "dest": 18, "amount": 50},
    {"src": 5, "dest": 4, "amount": 50},
    {"src": 1, "dest": 2, "amount": 50},
    {"src": 8, "dest": 9, "amount": 50},
    {"src": 10, "dest": 11, "amount": 50},
    {"src": 12, "dest": 13, "amount": 50},
    {"src": 14, "dest": 15, "amount": 50},
    {"src": 6, "dest": 7, "amount": 50},
]

# Create and start a thread for each transaction
for i, transaction in enumerate(transactions):
    thread_name = f'thread{i+1}'  # Create a unique thread name
    thread = threading.Thread(
        target=transact,
        args=(transaction["src"], transaction["dest"],
              transaction["amount"], thread_name)
    )
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()
