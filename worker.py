#!/usr/bin/env python3
import rpyc
import time
import os
import sys
import socket
import string
from collections import defaultdict, Counter
import random


def map_function(text):
    STOP_WORDS = set([
        'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in',
        'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with',
    ])
    TR = str.maketrans(string.punctuation, ' ' * len(string.punctuation))
    
    word_counts = defaultdict(int)
    
    for line in text.split('\n'):
        if line.lstrip().startswith('..'):
            continue
        line = line.translate(TR)
        for word in line.split():
            word = word.lower()
            if word.isalpha() and word not in STOP_WORDS:
                word_counts[word] += 1
    
    return dict(word_counts)

def reduce_function(key, values):
    return (key, sum(values))


def partition_function(key, n_reduce):
    return hash(key) % n_reduce


class Worker:
    def __init__(self, worker_id, coordinator_host, coordinator_port):
        self.worker_id = worker_id
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
        self.coordinator = None
        self.running = True
        
    def connect_to_coordinator(self):
        max_retries = 10
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                print(f"Attempting to connect to coordinator at {self.coordinator_host}:{self.coordinator_port} (attempt {attempt + 1}/{max_retries})")
                self.coordinator = rpyc.connect(
                    self.coordinator_host,
                    self.coordinator_port,
                    config={
                        'allow_public_attrs': True,
                        'allow_pickle': True,
                        'sync_request_timeout': 300
                    }
                )
                # Register with coordinator
                self.coordinator.root.register_worker(self.worker_id)
                print(f"Worker {self.worker_id} connected to coordinator")
                return True
            except Exception as e:
                print(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    print("Failed to connect to coordinator after all retries")
                    return False
        
        return False
    
    def execute_map_task(self, filepath, n_reduce):
        print(f"Executing map task on {filepath}")
        
        try:
            # Read input file
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read()
            
            # Apply map function (returns aggregated counts)
            word_counts = map_function(text)
            print(f"Map produced {len(word_counts)} unique words")
            
            # Partition results
            partitioned = defaultdict(list)
            for word, count in word_counts.items():
                region = partition_function(word, n_reduce)
                partitioned[region].append((word, count))
            
            print(f"Partitioned into {len(partitioned)} regions")
            return dict(partitioned)
            
        except Exception as e:
            print(f"Error executing map task: {e}")
            raise
    
    def execute_reduce_task(self, region, intermediate_data):
        print(f"Executing reduce task for region {region} with {len(intermediate_data)} pairs")
        
        try:
            # Group by key
            grouped = defaultdict(list)
            for key, value in intermediate_data:
                grouped[key].append(value)
            
            # Apply reduce function
            reduce_output = []
            for key, values in grouped.items():
                result = reduce_function(key, values)
                reduce_output.append(result)
            
            print(f"Reduce produced {len(reduce_output)} results")
            return reduce_output
            
        except Exception as e:
            print(f"Error executing reduce task: {e}")
            raise
    
    def run(self):
        print(f"Worker {self.worker_id} starting...")
        
        # Connect to coordinator
        if not self.connect_to_coordinator():
            print("Failed to connect to coordinator, exiting")
            return
        
        # Main task loop
        idle_count = 0
        max_idle = 30  # Exit after 30 seconds of no tasks
        
        while self.running:
            try:
                # Request task from coordinator
                task_info = self.coordinator.root.request_task(self.worker_id)
                
                if task_info is None:
                    idle_count += 1
                    if idle_count >= max_idle:
                        print("No more tasks available, shutting down")
                        break
                    time.sleep(1)
                    continue
                
                idle_count = 0  # Reset idle counter
                task_id, task_type, input_data = task_info
                
                if task_type == "map":
                    filepath, n_reduce = input_data
                    result = self.execute_map_task(filepath, n_reduce)
                    self.coordinator.root.submit_map_result(self.worker_id, task_id, result)
                    
                elif task_type == "reduce":
                    region, intermediate_data = input_data
                    result = self.execute_reduce_task(region, intermediate_data)
                    self.coordinator.root.submit_reduce_result(self.worker_id, task_id, result)
                
                print(f"Task {task_id} ({task_type}) completed successfully")
                
            except EOFError:
                print("Coordinator closed connection, shutting down")
                break
            except Exception as e:
                print(f"Error in worker loop: {e}")
                time.sleep(2)
                # Try to reconnect
                try:
                    self.connect_to_coordinator()
                except:
                    print("Failed to reconnect, exiting")
                    break
        
        print(f"Worker {self.worker_id} shutting down")


if __name__ == "__main__":
    # Use container hostname as worker ID 
    worker_id = os.environ.get('WORKER_ID')
    if not worker_id:
        try:
            worker_id = socket.gethostname()
        except:
            worker_id = f'worker-{random.randint(1000, 9999)}'
    
    coordinator_host = os.environ.get('COORDINATOR_HOST', 'coordinator')
    coordinator_port = int(os.environ.get('COORDINATOR_PORT', '18861'))
    
    print(f"Starting Worker {worker_id}")
    print(f"Coordinator: {coordinator_host}:{coordinator_port}")
    
    worker = Worker(worker_id, coordinator_host, coordinator_port)
    worker.run()
