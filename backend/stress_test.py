import asyncio
import aiohttp
import time
import uuid
import jwt
import os
import random
import sys

# MOCK JWT Configuration (Must match backend/security/jwt.py)
JWT_SECRET = os.getenv("JWT_SECRET", "super-secret-key") # Adjust to your actual secret
ALGORITHM = "HS256"

# Server URL
SERVER_URL = "http://127.0.0.1:8000/api/v1/ai/chat" 

# Yengilroq Limitlar (Windows uchun moslashgan)
TOTAL_REQUESTS = 500 
CONCURRENCY_LIMIT = 50 

def generate_mock_user_token(user_id: str, role: str = "pro"):
    payload = {
        "sub": user_id,
        "role": role,
        "exp": int(time.time()) + 3600
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=ALGORITHM)

async def fire_request(session: aiohttp.ClientSession, user_id: str, token: str, is_replay: bool = False, replay_key: str = None):
    headers = {
        "Authorization": f"Bearer {token}",
        "Idempotency-Key": replay_key if is_replay else str(uuid.uuid4())
    }
    
    payload = {
        "message": f"Hello, tell me a quick fact. Random id: {random.randint(1, 10000)}",
        "model": "gpt-4o",
        "language": "en"
    }

    start_time = time.time()
    try:
        async with session.post(SERVER_URL, json=payload, headers=headers) as response:
            status = response.status
            text = await response.text()
            latency = time.time() - start_time
            return {"status": status, "latency": latency, "text": text}
    except Exception as e:
        latency = time.time() - start_time
        return {"status": "ERROR", "latency": latency, "text": str(e)}

async def run_spike_attack():
    print("[*] INITIATING APEX AI SPIKE ATTACK [*]")
    print(f"Target: {SERVER_URL}")
    print(f"Total Requests: {TOTAL_REQUESTS} | Concurrency: {CONCURRENCY_LIMIT}")
    print("-----------------------------------------------------")
    
    # 1. Generate a pool of 20 users bounds
    users = [f"stress_user_{i}" for i in range(20)]
    tokens = {u: generate_mock_user_token(u) for u in users}
    
    # 2. Setup Idempotent Attack Keys
    replay_keys = [str(uuid.uuid4()) for _ in range(50)]
    
    connector = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for i in range(TOTAL_REQUESTS):
            user = random.choice(users)
            token = tokens[user]
            
            # 30% duplicate Idempotency replay attack
            is_replay = random.random() < 0.3
            r_key = random.choice(replay_keys) if is_replay else None
            
            tasks.append(fire_request(session, user, token, is_replay, r_key))
            
        print("[!] Launching payload...")
        start_time = time.time()
        
        # Attack!
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        total_time = time.time() - start_time
        
        # Analyze Results
        status_counts = {}
        latencies = []
        success_count = 0
        error_count = 0
        
        for r in results:
            if isinstance(r, Exception):
                status_counts["EXCEPTION"] = status_counts.get("EXCEPTION", 0) + 1
                error_count += 1
                continue
                
            s = r.get("status", "ERROR")
            status_counts[s] = status_counts.get(s, 0) + 1
            lat_ms = r.get("latency", 0) * 1000
            if "ERROR" not in str(s) and "EXCEPTION" not in str(s):
                latencies.append(lat_ms)
            
            if str(s).startswith("2"):
                success_count += 1
            else:
                error_count += 1

        rps = TOTAL_REQUESTS / total_time
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        latencies.sort()
        p95_latency = latencies[int(len(latencies) * 0.95)] if latencies else 0
        p99_latency = latencies[int(len(latencies) * 0.99)] if latencies else 0
        
        print("\n=== ATTACK REPORT ===")
        print(f"Time Taken  : {total_time:.2f} seconds")
        print(f"RPS         : {rps:.2f} req/sec")
        print(f"Avg Latency : {avg_latency:.2f} ms")
        print(f"P95 Latency : {p95_latency:.2f} ms")
        print(f"P99 Latency : {p99_latency:.2f} ms")
        print("-----------------------------------------------------")
        print("Response Codes:")
        for code, count in sorted(status_counts.items(), key=lambda x: str(x[0])):
            print(f" [{code}]: {count} hits")
        print("-----------------------------------------------------")
        
        if status_counts.get(429, 0) > 0:
            print("[SHIELD] RATE LIMITER: Actively Defended!")
        if status_counts.get(503, 0) > 0:
            print("[SHIELD] CIRCUIT BREAKER: Actively Defended!")
        if status_counts.get("ERROR", 0) > 0 or status_counts.get("EXCEPTION", 0) > 0:
            print("[WARN] NETWORK DROPS: Server or OS socket limits reached.")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run_spike_attack())
