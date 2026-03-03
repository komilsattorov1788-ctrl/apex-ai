import asyncio
import time
import hashlib
import random
import sys
from collections import deque

class SemanticCache:
    """
    DATA PLANE: L1 Cache. 
    Intercepts exact or semantically identical prompts before they ever touch the GPU.
    Reduces compute cost to $0.00 and latency to <5ms.
    """
    def __init__(self):
        self.cache = {}
        
    def _hash(self, prompt: str) -> str:
        # In a real system, this could use a quick small embedding model + FAISS/Redis Vector
        # Here we use exact matching (hash) + simple normalization
        normalized = prompt.lower().strip()
        return hashlib.md5(normalized.encode()).hexdigest()

    async def get(self, prompt: str):
        key = self._hash(prompt)
        # Simulate network/lookup latency for Redis Vector
        await asyncio.sleep(0.005) 
        if key in self.cache:
            return self.cache[key]
        return None

    async def set(self, prompt: str, response: str):
        key = self._hash(prompt)
        self.cache[key] = response

class ContinuousBatcher:
    """
    DATA PLANE: GPU Inference Scheduler (Simulating vLLM PagedAttention / Triton)
    Instead of processing 1 request = 1 GPU forward pass (which starves memory bandwidth),
    we dynamically batch incoming requests and process them together, maximizing GPU utilization.
    """
    def __init__(self, max_batch_size=16):
        self.queue = deque()
        self.max_batch_size = max_batch_size
        self.is_running = False

    async def submit_request(self, req_id: str, prompt: str) -> asyncio.Future:
        future = asyncio.Future()
        self.queue.append((req_id, prompt, time.time(), future))
        return future

    async def _mock_gpu_forward_pass(self, batch):
        """Simulates a heavy GPU operation for a dynamic batch of requests."""
        batch_size = len(batch)
        # The beauty of GPU batching: 16 requests take almost the same time as 1 request!
        # T_batch = T_single + (batch_size * tiny_overhead)
        base_time = 0.5 
        overhead = 0.02 * batch_size
        compute_time = base_time + overhead
        
        await asyncio.sleep(compute_time)
        
        # Fulfill futures
        for req_id, prompt, _, future in batch:
            result = f"[GPU] Generated text for '{prompt}' in batch of {batch_size}"
            if not future.done():
                future.set_result(result)

    async def run_scheduler(self):
        self.is_running = True
        print("[Data Plane] [START] Continuous Batching GPU Engine Started...")
        while self.is_running:
            if not self.queue:
                await asyncio.sleep(0.01) # fast poll
                continue
                
            batch = []
            while self.queue and len(batch) < self.max_batch_size:
                batch.append(self.queue.popleft())
                
            print(f"[GPU VRAM] Dispatching Batch Matrix of size {len(batch)} to Tensor Cores...")
            asyncio.create_task(self._mock_gpu_forward_pass(batch))
            
            # Allow event loop to process incoming requests before firing the next batch
            await asyncio.sleep(0.05) 

async def test_data_plane():
    print("======================================================================")
    print("=== DATA PLANE INCUBATION: SEMANTIC CACHE & CONTINUOUS BATCHING ====")
    print("======================================================================\n")
    
    cache = SemanticCache()
    batcher = ContinuousBatcher(max_batch_size=8)
    
    # Start the GPU event loop
    scheduler_task = asyncio.create_task(batcher.run_scheduler())
    
    # Simulate a sudden thundering herd of 20 concurrent AI requests
    print("[INCOMING] THUNDERING HERD of 20 concurrent requests (RPS spike)...\n")
    
    # We pre-warm the cache with one common prompt to show L1 hits
    await cache.set("What is the capital of France?", "[Cache] Paris")
    
    prompts = [
        "What is the capital of France?", # Cache Hit
        "Explain Quantum Physics in 1 paragraph.", 
        "Write a python script to reverse a string.",
        "What is the capital of France?", # Cache Hit
        "Tell me a joke.",
        "How to bake a cake?",
        "What is the capital of France?", # Cache Hit
        "Explain sorting algorithms.",
        "Who is the president of the moon?",
        "Write a poem about AI.",
        "What is the capital of France?", # Cache Hit
        "Translate 'hello' to Spanish.",
        "What is 2 + 2?",
        "Summarize the plot of Inception.",
        "What is the capital of France?", # Cache Hit
        "Write a React component for a button.",
        "How do airplanes fly?",
        "What is the capital of France?", # Cache Hit
        "Write a SQL query to join two tables.",
        "Draft an email to my boss."
    ]

    async def process_request(i, prompt):
        start_time = time.time()
        
        # 1. Check L1 Data Plane (Semantic Cache)
        cached_res = await cache.get(prompt)
        if cached_res:
            latency = (time.time() - start_time) * 1000
            print(f"[HIT] Req {i}: Served from Cache in {latency:.1f}ms -> {cached_res}")
            return
            
        # 2. Forward to L2 Data Plane (GPU Batcher)
        print(f"[MISS] Req {i}: Forwarding '{prompt}' to GPU Queue...")
        future = await batcher.submit_request(f"req_{i}", prompt)
        res = await future
        
        # 3. Cache the result for next time
        await cache.set(prompt, res)
        
        latency = (time.time() - start_time) * 1000
        print(f"[GPU] Req {i}: Completed via Tensor Batch in {latency:.1f}ms")

    # Blast all 20 requests concurrently
    tasks = [process_request(i, p) for i, p in enumerate(prompts)]
    await asyncio.gather(*tasks)
    
    batcher.is_running = False
    scheduler_task.cancel()
    
    print("\n[OK] DATA PLANE TEST COMPLETE.")
    print("Notice how cache hits took <10ms, and GPU requests were processed efficiently in BATCHES rather than starving the VRAM sequentially!")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(test_data_plane())
