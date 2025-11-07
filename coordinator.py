import rpyc
from rpyc.utils.server import ThreadedServer
import threading
import time
import os
import sys
import requests
import zipfile
import subprocess
from collections import Counter, defaultdict


class Task:
    def __init__(self, task_id, task_type, input_data):
        self.task_id = task_id
        self.task_type = task_type
        self.input_data = input_data
        self.assigned_worker = None
        self.start_time = None
        self.completed = False
        self.result = None


class CoordinatorService(rpyc.Service):
    def __init__(self):
        super().__init__()
        self.tasks = {}
        self.map_tasks = []
        self.reduce_tasks = []
        self.available_tasks = []
        self.task_lock = threading.Lock()
        self.results_lock = threading.Lock()
        self.map_results = defaultdict(list)
        self.reduce_results = []
        self.task_counter = 0
        self.n_reduce = 0
        self.map_phase_complete = False
        self.all_complete = False
        
    def exposed_register_worker(self, worker_id):
        print(f"Worker {worker_id} registered")
        return True
    
    def exposed_request_task(self, worker_id):
        with self.task_lock:
            # Check if all tasks are complete
            if self.all_complete:
                return None
            
            # Find an available task
            for task in self.available_tasks[:]:
                if not task.completed and task.assigned_worker is None:
                    task.assigned_worker = worker_id
                    task.start_time = time.time()
                    print(f"Assigned {task.task_type} task {task.task_id} to {worker_id}")
                    return (task.task_id, task.task_type, task.input_data)
            
            # Check for timed-out tasks
            for task in self.available_tasks:
                if not task.completed and task.assigned_worker is not None:
                    if time.time() - task.start_time > 20:  # 20 second timeout
                        print(f"Task {task.task_id} timed out on {task.assigned_worker}, reassigning to {worker_id}")
                        task.assigned_worker = worker_id
                        task.start_time = time.time()
                        return (task.task_id, task.task_type, task.input_data)
            
            return None
    
    def exposed_submit_map_result(self, worker_id, task_id, partitioned_data):
        print(f"Received map result from {worker_id} for task {task_id}")
        
        with self.task_lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                if not task.completed:
                    task.completed = True
                    task.result = partitioned_data
                    
                    # Store partitioned results by region
                    with self.results_lock:
                        for region, pairs in partitioned_data.items():
                            self.map_results[region].extend(pairs)
                    
                    print(f"Map task {task_id} completed by {worker_id}")
                    
                    # Check if all map tasks are done
                    if all(t.completed for t in self.map_tasks):
                        self._start_reduce_phase()
        
        return True
    
    def exposed_submit_reduce_result(self, worker_id, task_id, results):
        print(f"Received reduce result from {worker_id} for task {task_id}")
        
        with self.task_lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                if not task.completed:
                    task.completed = True
                    task.result = results
                    
                    with self.results_lock:
                        self.reduce_results.extend(results)
                    
                    print(f"Reduce task {task_id} completed by {worker_id}")
                    
                    # Check if all reduce tasks are done
                    if all(t.completed for t in self.reduce_tasks):
                        self.all_complete = True
                        print("All tasks completed!")
        
        return True
    
    def _start_reduce_phase(self):
        print("Starting reduce phase...")
        self.map_phase_complete = True
        
        # Create reduce tasks for each region
        for region in range(self.n_reduce):
            task_id = self.task_counter
            self.task_counter += 1
            
            # Get all intermediate data for this region
            intermediate_data = self.map_results.get(region, [])
            
            task = Task(task_id, "reduce", (region, intermediate_data))
            self.tasks[task_id] = task
            self.reduce_tasks.append(task)
            self.available_tasks.append(task)
        
        print(f"Created {len(self.reduce_tasks)} reduce tasks")
    
    def initialize_map_tasks(self, text_files, n_reduce):
        self.n_reduce = n_reduce
        
        for i, filepath in enumerate(text_files):
            task_id = self.task_counter
            self.task_counter += 1
            
            task = Task(task_id, "map", (filepath, n_reduce))
            self.tasks[task_id] = task
            self.map_tasks.append(task)
            self.available_tasks.append(task)
        
        print(f"Created {len(self.map_tasks)} map tasks from {len(text_files)} files")
    
    def wait_for_completion(self):
        while not self.all_complete:
            time.sleep(1)
        
        return self.reduce_results


def download_dataset(url, output_dir="txt"):
    os.makedirs(output_dir, exist_ok=True)
    
    filename = url.split('/')[-1]
    filepath = os.path.join(output_dir, filename)
    
    # Skip if already downloaded
    if os.path.exists(filepath.replace('.zip', '')):
        print(f"Dataset already exists at {filepath.replace('.zip', '')}")
        return filepath.replace('.zip', '')
    
    if not os.path.exists(filepath):
        print(f"Downloading {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded to {filepath}")
    
    # Extract if it's a zip file
    if filepath.endswith('.zip'):
        print(f"Extracting {filepath}...")
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
        
        extracted_file = filepath.replace('.zip', '')
        print(f"Extracted to {extracted_file}")
        return extracted_file
    
    return filepath


def split_file(input_file, num_splits, output_dir="input_splits"):
    os.makedirs(output_dir, exist_ok=True)
    
    file_size = os.path.getsize(input_file)
    split_size = file_size // num_splits
    
    prefix = os.path.join(output_dir, "split_")
    
    subprocess.run([
        'split',
        '-b', str(split_size),
        '-d',
        '--additional-suffix=.txt',
        input_file,
        prefix
    ], check=True)
    
    split_files = sorted([
        os.path.join(output_dir, f)
        for f in os.listdir(output_dir)
        if f.startswith('split_') and f.endswith('.txt')
    ])
    
    for i, split_file in enumerate(split_files):
        file_size = os.path.getsize(split_file)
        print(f"Created split {i}: {split_file} ({file_size} bytes)")
    
    return split_files


if __name__ == "__main__":
    
    # Configuration
    dataset_url = os.environ.get('DATASET_URL', 'http://mattmahoney.net/dc/enwik8.zip')
    n_reduce = int(os.environ.get('N_REDUCE', '3'))
    n_map = int(os.environ.get('N_MAP', '6'))
    port = int(os.environ.get('COORDINATOR_PORT', '18861'))
    
    print(f"Starting Coordinator on port {port}")
    print(f"Dataset URL: {dataset_url}")
    print(f"Number of map tasks: {n_map}")
    print(f"Number of reduce workers: {n_reduce}")
    
    # Download and prepare dataset
    dataset_file = download_dataset(dataset_url)
    
    # Split file into chunks for map tasks
    split_files = split_file(dataset_file, n_map)

    start_time = time.time()
    
    # Create coordinator service
    coordinator_service = CoordinatorService()
    coordinator_service.initialize_map_tasks(split_files, n_reduce)
    
    # Start RPyC server in a separate thread
    server = ThreadedServer(
        coordinator_service,
        port=port,
        protocol_config={
            'allow_public_attrs': True,
            'allow_pickle': True,
            'sync_request_timeout': 300
        }
    )
    
    server_thread = threading.Thread(target=server.start, daemon=True)
    server_thread.start()
    print(f"Coordinator server started on port {port}")
    
    # Wait for all tasks to complete
    results = coordinator_service.wait_for_completion()
    
    # Aggregate and display results
    print("Aggregating final results...")
    word_counts = Counter()
    for word, count in results:
        word_counts[word] += count
    
    # Display top 20 most frequent words
    print('\nTOP 20 WORDS BY FREQUENCY\n')
    
    top20 = word_counts.most_common(20)
    longest = max(len(word) for word, count in top20)
    
    i = 1
    for word, count in top20:
        print('%s.\t%-*s: %5s' % (i, longest+1, word, count))
        i = i + 1

    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print("Elapsed Time: {} seconds".format(elapsed_time))
    
    # Shutdown server
    server.close()
    print("Coordinator shutting down")
