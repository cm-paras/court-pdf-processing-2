"""
Smart hybrid fetcher: Tries simple approach first, falls back to partitioning if needed.
Auto-detects the best strategy based on document count.
"""
import json
import requests
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config.settings import AZURE_CONFIG

class ProgressTracker:
    def __init__(self, total):
        self.total = total
        self.current = 0
        self.lock = Lock()
    
    def update(self, count):
        with self.lock:
            self.current += count
            pct = (self.current / self.total * 100) if self.total > 0 else 0
            print(f"   📊 Progress: {self.current:,} / {self.total:,} ({pct:.1f}%)")

def fetch_simple(headers, search_url, total_docs):
    """Try simple orderby pagination (works up to 100K docs)."""
    print("\n📋 Strategy 1: Simple orderby pagination")
    print("   (Works if total <= 100K docs)\n")
    
    all_chunks = []
    batch_size = 1000
    skip = 0
    start_time = time.time()
    
    while len(all_chunks) < total_docs:
        payload = {
            "search": "*",
            "select": "pdf_id,chunk_index,chunk_total",
            "orderby": "id asc",
            "top": batch_size,
            "skip": skip,
            "count": False
        }
        
        response = requests.post(search_url, headers=headers, json=payload, timeout=30)
        
        if response.status_code != 200:
            print(f"   ❌ Error at skip={skip}: {response.status_code}")
            return None
        
        batch = response.json().get("value", [])
        
        if not batch:
            break
        
        all_chunks.extend(batch)
        
        if len(all_chunks) % 10000 == 0:
            elapsed = time.time() - start_time
            pct = len(all_chunks) / total_docs * 100
            print(f"   📊 {len(all_chunks):,} / {total_docs:,} ({pct:.1f}%) - {len(all_chunks)/elapsed:.0f} docs/sec")
        
        if len(batch) < batch_size:
            break
        
        skip += batch_size
        
        if skip >= 100000:
            print(f"\n   ⚠️ Hit 100K skip limit at {len(all_chunks):,} docs")
            return None  # Signal to use partition approach
        
        time.sleep(0.02)
    
    return all_chunks

def get_unique_pdf_ids(headers, search_url):
    """Get unique pdf_ids using facets."""
    payload = {
        "search": "*",
        "facets": ["pdf_id,count:100000"],
        "top": 0
    }
    
    response = requests.post(search_url, headers=headers, json=payload)
    
    if response.status_code != 200:
        return None
    
    facets = response.json().get("@search.facets", {}).get("pdf_id", [])
    return [f["value"] for f in facets]

def fetch_chunks_for_pdf(headers, search_url, pdf_id, progress):
    """Fetch chunks for one pdf_id."""
    chunks = []
    skip = 0
    batch_size = 1000
    
    while skip < 100000:
        payload = {
            "search": "*",
            "filter": f"pdf_id eq '{pdf_id}'",
            "select": "pdf_id,chunk_index,chunk_total",
            "top": batch_size,
            "skip": skip,
            "orderby": "chunk_index asc"
        }
        
        try:
            response = requests.post(search_url, headers=headers, json=payload, timeout=30)
            
            if response.status_code != 200:
                break
            
            batch = response.json().get("value", [])
            
            if not batch:
                break
            
            chunks.extend(batch)
            
            if len(batch) < batch_size:
                break
            
            skip += batch_size
            time.sleep(0.01)
            
        except:
            break
    
    if chunks:
        progress.update(len(chunks))
    
    return chunks

def fetch_partition(headers, search_url, pdf_ids, partition_num, progress):
    """Fetch chunks for a partition of pdf_ids."""
    all_chunks = []
    
    for i, pdf_id in enumerate(pdf_ids, 1):
        chunks = fetch_chunks_for_pdf(headers, search_url, pdf_id, progress)
        all_chunks.extend(chunks)
        
        if i % 200 == 0:
            print(f"   • Partition {partition_num}: {i:,}/{len(pdf_ids):,} PDFs")
    
    return all_chunks

def fetch_partitioned(headers, search_url, total_docs):
    """Fetch using pdf_id partitioning (works for any size)."""
    print("\n📋 Strategy 2: Partition by pdf_id")
    print("   (Works for unlimited docs)\n")
    
    print("🔍 Discovering unique pdf_ids...")
    pdf_ids = get_unique_pdf_ids(headers, search_url)
    
    if not pdf_ids:
        print("   ❌ Could not get pdf_ids")
        return None
    
    print(f"   ✅ Found {len(pdf_ids):,} unique PDFs\n")
    
    # Create partitions
    num_threads = 8
    partition_size = len(pdf_ids) // num_threads + 1
    partitions = [
        pdf_ids[i:i+partition_size] 
        for i in range(0, len(pdf_ids), partition_size)
    ]
    
    print(f"🔀 Using {num_threads} threads, {len(partitions)} partitions")
    
    progress = ProgressTracker(total_docs)
    all_chunks = []
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = {
            executor.submit(fetch_partition, headers, search_url, p, i+1, progress): i 
            for i, p in enumerate(partitions)
        }
        
        for future in as_completed(futures):
            try:
                chunks = future.result()
                all_chunks.extend(chunks)
            except Exception as e:
                print(f"   ⚠️ Partition failed: {e}")
    
    return all_chunks

def main():
    config = AZURE_CONFIG
    api_version = "2023-11-01"
    index_name = config.SEARCH_INDEX_NAME
    endpoint = config.SEARCH_ENDPOINT
    api_key = config.SEARCH_KEY
    
    headers = {
        "Content-Type": "application/json",
        "api-key": api_key
    }
    
    search_url = f"{endpoint}/indexes/{index_name}/docs/search?api-version={api_version}"
    
    print(f"{'='*70}")
    print(f"🚀 SMART Fetcher - Auto-selects best strategy")
    print(f"{'='*70}\n")
    
    # Get total count
    count_response = requests.post(
        search_url,
        headers=headers,
        json={"search": "*", "top": 0, "count": True}
    )
    
    total_docs = count_response.json().get("@odata.count", 0)
    print(f"📊 Total documents: {total_docs:,}")
    
    # Auto-select strategy
    if total_docs <= 100000:
        print(f"✅ Can use simple pagination (total <= 100K)")
        strategy = "simple"
    else:
        print(f"⚠️ Must use partitioned approach (total > 100K)")
        strategy = "partition"
    
    start_time = time.time()
    
    # Try selected strategy
    if strategy == "simple":
        all_chunks = fetch_simple(headers, search_url, total_docs)
        
        # Fallback to partition if simple failed
        if all_chunks is None:
            print("\n⚠️ Simple approach failed, switching to partitioned...")
            all_chunks = fetch_partitioned(headers, search_url, total_docs)
    else:
        all_chunks = fetch_partitioned(headers, search_url, total_docs)
    
    elapsed = time.time() - start_time
    
    if not all_chunks:
        print("\n❌ Failed to fetch documents")
        return
    
    print(f"\n{'='*70}")
    print(f"✅ Fetch complete in {elapsed:.1f}s")
    print(f"   Retrieved: {len(all_chunks):,} / {total_docs:,}")
    print(f"   Speed: {len(all_chunks)/elapsed:.0f} docs/sec")
    print(f"{'='*70}\n")
    
    # Remove duplicates (just in case)
    unique_chunks = {doc['pdf_id'] + str(doc['chunk_index']): doc for doc in all_chunks}
    all_chunks = list(unique_chunks.values())
    
    # Save files
    print("💾 Saving files...")
    
    with open("chunks_metadata.jsonl", "w", encoding="utf-8") as f:
        for doc in all_chunks:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
    print(f"   ✅ chunks_metadata.jsonl")
    
    with open("chunks_metadata.json", "w", encoding="utf-8") as f:
        json.dump(all_chunks, f, ensure_ascii=False, indent=2)
    print(f"   ✅ chunks_metadata.json")
    
    import csv
    with open("chunks_metadata.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=['pdf_id', 'chunk_index', 'chunk_total'])
        writer.writeheader()
        writer.writerows(all_chunks)
    print(f"   ✅ chunks_metadata.csv")
    
    print(f"\n📊 Final Summary:")
    print(f"   - Documents: {len(all_chunks):,} / {total_docs:,}")
    print(f"   - Coverage: {len(all_chunks)/total_docs*100:.2f}%")
    print(f"   - Time: {elapsed:.1f}s")
    print(f"   - Speed: {len(all_chunks)/elapsed:.0f} docs/sec")
    
    if len(all_chunks) < total_docs:
        print(f"\n⚠️ Missing {total_docs - len(all_chunks):,} documents")

if __name__ == "__main__":
    main()