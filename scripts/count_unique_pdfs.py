"""
Count unique pdf_ids in Azure Search index using facets.
"""
import requests
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config.settings import AZURE_CONFIG

def count_unique_pdfs():
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
    
    print(f"{'='*60}")
    print(f"Counting unique pdf_ids in: {index_name}")
    print(f"{'='*60}\n")
    
    # Get total document count
    count_payload = {
        "search": "*",
        "top": 0,
        "count": True
    }
    
    count_response = requests.post(search_url, headers=headers, json=count_payload)
    
    if count_response.status_code != 200:
        print(f"❌ Error getting document count: {count_response.status_code}")
        print(f"   {count_response.text}")
        return
    
    total_docs = count_response.json().get("@odata.count", 0)
    print(f"📄 Total documents (chunks): {total_docs:,}")
    
    # Get unique pdf_ids using facets
    facet_payload = {
        "search": "*",
        "facets": ["pdf_id,count:500000"],  # Get up to 500K unique values
        "top": 0
    }
    
    print(f"🔍 Discovering unique pdf_ids...")
    
    facet_response = requests.post(search_url, headers=headers, json=facet_payload)
    
    if facet_response.status_code != 200:
        print(f"❌ Error getting facets: {facet_response.status_code}")
        print(f"   {facet_response.text}")
        return
    
    data = facet_response.json()
    facets = data.get("@search.facets", {}).get("pdf_id", [])
    
    unique_count = len(facets)
    
    print(f"\n{'='*60}")
    print(f"✅ RESULT:")
    print(f"{'='*60}")
    print(f"📊 Unique pdf_ids: {unique_count:,}")
    print(f"📄 Total chunks: {total_docs:,}")
    print(f"📈 Avg chunks per PDF: {total_docs/unique_count:.1f}")
    print(f"{'='*60}\n")
    
    # Show top 5 PDFs by chunk count
    if facets:
        print("🔝 Top 5 PDFs by chunk count:")
        sorted_facets = sorted(facets, key=lambda x: x['count'], reverse=True)[:5]
        for i, facet in enumerate(sorted_facets, 1):
            pdf_name = facet['value'].split('/')[-1]  # Get filename only
            print(f"   {i}. {pdf_name[:50]}: {facet['count']:,} chunks")

if __name__ == "__main__":
    count_unique_pdfs()