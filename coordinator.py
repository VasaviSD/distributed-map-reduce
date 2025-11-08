import rpyc
from rpyc.utils.classic import obtain
import time
import os
import requests
import zipfile
import subprocess
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor

class Task:
    def __init__(self, task_id, input_data):
        self.task_id = task_id
        self.input_data = input_data
        self.assigned_worker = None
        self.start_time = None
        self.completed = False
        self.result = None

class Worker:
    def __init__(self, worker_id, conn):
        self.worker_id = worker_id
        self.conn = conn
        self.busy = False
        self.future = None


class CoordinatorService(rpyc.Service):
    def __init__(self, n_map, n_reduce):
        super().__init__()
        self.map_tasks = {}
        self.reduce_tasks = {}
        self.map_results = defaultdict(list)
        self.reduce_results = []
        self.n_map = n_map
        self.n_reduce = n_reduce
        self.workers = {}
        
    def connect_to_workers(self, worker_port):
        print("Connecting to workers...")
        
        i = 1
        while True:
            worker_host = f"distributed-map-reduce-worker-{i}"
            try:
                print(f"Trying to connect to {worker_host}:{worker_port}...")
                conn = rpyc.connect(
                    worker_host,
                    worker_port,
                    config={
                        'allow_public_attrs': True,
                        'allow_pickle': True,
                        'sync_request_timeout': 300
                    }
                )
                worker = Worker(worker_host, conn)

                self.workers[worker_host] = worker
                print(f"Connected to {worker_host}")
                i += 1
            except Exception as e:
                print(f"Could not connect to {worker_host}: {e}")
                break
        
        print(f"Connected to {len(self.workers)} workers")
        return self.workers
    

    def close_workers(self):
        print("Attempting to close workers....")
        
        for i in range(1, len(self.workers) + 1):
            worker_host = f"distributed-map-reduce-worker-{i}"
            print(f"Trying to close {worker_host}...")
            try:
                self.workers[worker_host].conn.root.exposed_close()
            except Exception as e:
                continue
    
    def is_phase_complete(self, phase):
        if phase == "map":
            tasks = self.map_tasks
        else:
            tasks = self.reduce_tasks
        
        for task in tasks:
            if not tasks[task].completed:
                return False
        return True

    def get_available_tasks(self, phase):
        tasks = []
        if phase == "map":
            for task in self.map_tasks:
                if not self.map_tasks[task].assigned_worker and not self.map_tasks[task].completed:
                    tasks.append(task)
        elif phase == "reduce":
            for task in self.reduce_tasks:
                if not self.reduce_tasks[task].assigned_worker and not self.reduce_tasks[task].completed:
                    tasks.append(task)
        
        return tasks
    
    def assign_task(self, task_id, worker_id, phase):
        if phase == "map":
            self.map_tasks[task_id].assigned_worker = worker_id
            self.map_tasks[task_id].start_time = time.time()
        elif phase == "reduce":
            self.reduce_tasks[task_id].assigned_worker = worker_id
            self.reduce_tasks[task_id].start_time = time.time()
        self.workers[worker_id].busy = True

    def get_free_worker(self):
        for worker_id in self.workers:
            if not self.workers[worker_id].busy and not self.workers[worker_id].conn.closed:
                return worker_id

    def is_future_success(self, future):
        return future.done() and not future.cancelled() and future.exception() is None
    
    def capture_results(self, phase):
        if phase == "map":
            tasks = self.map_tasks
        else:
            tasks = self.reduce_tasks

        for task_id in tasks:
            if tasks[task_id].assigned_worker is not None:
                worker_id = tasks[task_id].assigned_worker
                if time.time() - tasks[task_id].start_time > 20:  # 20 second timeout
                    print(f"Task {task_id} timed out on {tasks[task_id].assigned_worker}")
                    tasks[task_id].assigned_worker = None
                    self.workers[worker_id].busy = False
                    self.workers[worker_id].future = None
                    continue

                future = self.workers[worker_id].future

                if future and self.is_future_success(future):
                    if phase == "map":
                        result = obtain(future.result())
                        print(f"Received map result from {worker_id} for task {task_id}")
                            
                        # Store file locations by region
                        for region, filepath in result.items():
                            self.map_results[region].append(filepath)
                        
                        print(f"Map task {task_id} completed by {worker_id}, stored {len(result)} file locations")
                    else:
                        result = obtain(future.result())

                        print(f"Received reduce result from {worker_id} for task {task_id}")
                            
                        self.reduce_results.append(result)
                        
                        print(f"Reduce task {task_id} completed by {worker_id}, stored {len(result)} file locations")

                    tasks[task_id].completed = True
                    self.workers[worker_id].busy = False
                    self.workers[worker_id].future = None
                    tasks[task_id].assigned_worker = None

    
    def run_map_phase(self):
        print(f"Starting map phase with: {len(self.map_tasks)} tasks, {len(self.workers)} workers")
        
        executor = ThreadPoolExecutor(max_workers=len(self.workers))

        while not self.is_phase_complete("map"):
            for task_id in self.get_available_tasks("map"):
                if task_id is None:
                    continue

                worker_id = self.get_free_worker()

                if worker_id is None:
                    continue

                filepath = self.map_tasks[task_id].input_data
                print(f"Assigning map task {task_id} to {worker_id}: {filepath}")
                self.assign_task(task_id, worker_id, "map")

                future = executor.submit(
                        self.workers[worker_id].conn.root.exposed_map,
                        task_id,
                        filepath,
                        self.n_reduce
                    )
                self.workers[worker_id].future = future
            
            self.capture_results("map")
        
        print(f"Map phase complete: {len(self.map_results)} regions")
    
    def run_reduce_phase(self):
        print(f"Starting reduce phase with: {self.n_reduce} tasks, {len(self.workers)} workers")
        
        executor = ThreadPoolExecutor(max_workers=len(self.workers))

        while not self.is_phase_complete("reduce"):
            for task_id in self.get_available_tasks("reduce"):
                if task_id is None:
                    continue

                worker_id = self.get_free_worker()

                if worker_id is None:
                    continue

                files = self.reduce_tasks[task_id].input_data
                print(f"Assigning reduce task {task_id} to {worker_id}: {len(files)} files")
                self.assign_task(task_id, worker_id, "reduce")

                future = executor.submit(
                        self.workers[worker_id].conn.root.exposed_reduce,
                        task_id,
                        task_id,
                        files
                    )
                self.workers[worker_id].future = future
            
            self.capture_results("reduce")
        
        print(f"Reduce phase complete: {len(self.map_results)} regions")
    
    
    def initialize_reduce_tasks(self):
        for i in range(self.n_reduce):
            task = Task(i, self.map_results[i])
            self.reduce_tasks[i] = task
        
        print(f"Created {len(self.reduce_tasks)} reduce tasks from {self.n_reduce} regions")
    
    def initialize_map_tasks(self, text_files):
        for i, filepath in enumerate(text_files):
            task = Task(i, filepath)
            self.map_tasks[i] = task
        
        print(f"Created {len(self.map_tasks)} map tasks from {len(text_files)} files")
    
    def get_final_results(self):
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
    
    dataset_url = os.environ.get('DATASET_URL', 'http://mattmahoney.net/dc/enwik9.zip')
    n_reduce = int(os.environ.get('N_REDUCE', '3'))
    n_map = int(os.environ.get('N_MAP', '6'))
    worker_port = int(os.environ.get('WORKER_PORT', '18861'))
    
    print(f"Dataset URL: {dataset_url}")
    print(f"Number of map tasks: {n_map}")
    print(f"Number of reduce tasks: {n_reduce}")
    
    # Download and prepare dataset
    dataset_file = download_dataset(dataset_url)
    
    # Split file into chunks for map tasks
    split_files = split_file(dataset_file, n_map)

    start_time = time.time()
    
    coordinator_service = CoordinatorService(n_map, n_reduce)
    coordinator_service.initialize_map_tasks(split_files)
    coordinator_service.connect_to_workers(worker_port)

    coordinator_service.run_map_phase()
    coordinator_service.initialize_reduce_tasks()
    coordinator_service.run_reduce_phase()
    
    output_files = coordinator_service.get_final_results()
    
    # Aggregate and display results by reading from output files
    print("Aggregating final results from output files...")
    word_counts = Counter()
    for output_file in output_files:
        print(f"Reading results from {output_file}")
        with open(output_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split('\t')
                if len(parts) == 2:
                    word, count = parts[0], int(parts[1])
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

    coordinator_service.close_workers()
