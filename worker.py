import rpyc
import os
import socket
import string
from collections import defaultdict
import random
from rpyc.utils.server import ThreadedServer

class MapReduceService(rpyc.Service):

    def map_function(self, text):
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
    
    def exposed_map(self, task_id, filepath, n_reduce):
        print(f"Executing map task {task_id} on {filepath}")
        
        try:
            # Read input file
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read()
            
            # Apply map function (returns aggregated counts)
            word_counts = self.map_function(text)
            print(f"Map produced {len(word_counts)} unique words")
            
            # Write intermediate files (one per reduce region)
            intermediate_dir = "/app/intermediate"
            os.makedirs(intermediate_dir, exist_ok=True)
            
            file_locations = {}
            for region in range(n_reduce):
                intermediate_file = os.path.join(intermediate_dir, f"mr-{task_id}-{region}.txt")
                
                # Write only the pairs for this region
                with open(intermediate_file, 'w') as f:
                    for word, count in word_counts.items():
                        if self.partition_function(word, n_reduce) == region:
                            f.write(f"{word}\t{count}\n")
                
                file_locations[region] = intermediate_file
            
            print(f"Wrote intermediate files for {len(file_locations)} regions")
            return file_locations
            
        except Exception as e:
            print(f"Error executing map task: {e}")
            raise

    def reduce_function(self, key, values):
        return (key, sum(values))


    def partition_function(self, key, n_reduce):
        return hash(key) % n_reduce
    
    def exposed_reduce(self, task_id, region, intermediate_files):
        print(f"Executing reduce task {task_id} for region {region} with {len(intermediate_files)} files")
        
        try:
            # Read all intermediate files for this region
            grouped = defaultdict(list)
            
            for filepath in intermediate_files:
                if not os.path.exists(filepath):
                    print(f"Warning: file {filepath} not found")
                    continue
                    
                with open(filepath, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        parts = line.split('\t')
                        if len(parts) == 2:
                            word, count = parts[0], int(parts[1])
                            grouped[word].append(count)
            
            # Apply reduce function
            reduce_output = []
            for key, values in grouped.items():
                result = self.reduce_function(key, values)
                reduce_output.append(result)
            
            print(f"Reduce produced {len(reduce_output)} results from {len(grouped)} unique keys")
            
            # Write results to output file
            output_dir = "/app/output"
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, f"reduce-output-{task_id}.txt")
            
            with open(output_file, 'w') as f:
                for word, count in reduce_output:
                    f.write(f"{word}\t{count}\n")
            
            print(f"Wrote {len(reduce_output)} results to {output_file}")
            return output_file
            
        except Exception as e:
            print(f"Error executing reduce task: {e}")
            raise
    
    def exposed_close(self):
        t.close()


if __name__ == "__main__":
    worker_id = os.environ.get('WORKER_ID')
    if not worker_id:
        try:
            worker_id = socket.gethostname()
        except:
            worker_id = f'worker-{random.randint(1000, 9999)}'
    print(f"Starting worker {worker_id}")
    t = ThreadedServer(
        MapReduceService,
        port=18861,
        protocol_config={
            'allow_public_attrs': True,
            'allow_pickle': True,
            'sync_request_timeout': 300
        }
    )
    t.start()
    print(f"Stopped worker {worker_id}")
