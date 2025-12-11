#!/usr/bin/env python3
"""
PDF Processing Pipeline Status Checker

This script provides comprehensive status information about:
- Document metadata extraction (Cosmos DB)
- Document indexing (Azure Cognitive Search)
- Overall pipeline progress
"""

import sys
import os
import json
import argparse
from datetime import datetime
from collections import defaultdict

# Add project root to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.config.config import Config
from src.clients.azure_clients import AzureClientManager
from src.storage.cosmos_storage import CosmosStorage
from src.storage.search_indexer import SearchIndexer
import logging

logging.basicConfig(level=logging.INFO)


class PipelineStatusChecker:
    """Checks status of the PDF processing pipeline"""
    
    def __init__(self):
        """Initialize the status checker"""
        try:
            self.config = Config()
            self.config.validate()
            self.azure_clients = AzureClientManager(self.config)
            self.cosmos_storage = CosmosStorage(self.azure_clients, self.config)
            self.search_indexer = SearchIndexer(self.azure_clients, self.config)
            
            print("✅ Successfully connected to Azure services")
        except Exception as e:
            print(f"❌ Error initializing status checker: {e}")
            raise
    
    def check_cosmos_status(self):
        """Check Cosmos DB status and metadata extraction progress"""
        print("\n" + "="*60)
        print("📊 COSMOS DB STATUS - METADATA EXTRACTION")
        print("="*60)
        
        try:
            # Query all documents with timeout
            print("Querying Cosmos DB...")
            all_documents = list(self.cosmos_storage.container.query_items(
                query="SELECT * FROM c",
                enable_cross_partition_query=True,
                max_item_count=1000
            ))
            
            if not all_documents:
                print("❌ No documents found in Cosmos DB")
                return {"total": 0, "with_metadata": 0, "without_metadata": 0}
            
            # Analyze documents
            stats = {
                "total": len(all_documents),
                "with_metadata": 0,
                "without_metadata": 0,
                "metadata_fields": defaultdict(int)
            }
            
            documents_without_metadata = []
            
            for doc in all_documents:
                blob_name = doc.get("blob_name", "Unknown")
                metadata = doc.get("metadata", {})
                
                if metadata and isinstance(metadata, dict) and len(metadata) > 0:
                    stats["with_metadata"] += 1
                    
                    # Count metadata fields
                    for field in metadata.keys():
                        stats["metadata_fields"][field] += 1
                else:
                    stats["without_metadata"] += 1
                    documents_without_metadata.append(blob_name)
            
            # Display results
            print(f"📈 Total Documents: {stats['total']}")
            print(f"✅ With Metadata: {stats['with_metadata']} ({stats['with_metadata']/stats['total']*100:.1f}%)")
            print(f"❌ Without Metadata: {stats['without_metadata']} ({stats['without_metadata']/stats['total']*100:.1f}%)")
            
            if stats["metadata_fields"]:
                print(f"\n📋 Metadata Fields Distribution:")
                for field, count in sorted(stats["metadata_fields"].items()):
                    print(f"   • {field}: {count} documents ({count/stats['with_metadata']*100:.1f}%)")
            
            if documents_without_metadata:
                print(f"\n⚠️  Documents without metadata (first 10):")
                for doc in documents_without_metadata[:10]:
                    print(f"   • {doc}")
                if len(documents_without_metadata) > 10:
                    print(f"   ... and {len(documents_without_metadata) - 10} more")
            
            return stats
            
        except Exception as e:
            print(f"❌ Error checking Cosmos DB status: {e}")
            return {"error": str(e)}
    
    def check_search_index_status(self):
        """Check Azure Cognitive Search index status"""
        print("\n" + "="*60)
        print("🔍 AZURE COGNITIVE SEARCH STATUS")
        print("="*60)
        
        try:
            # Check if index exists
            search_client = self.azure_clients.search_client
            
            # Get index statistics
            try:
                # Query total document count with timeout
                print("Querying search index...")
                results = search_client.search(
                    search_text="*",
                    include_total_count=True,
                    top=0
                )
                
                total_indexed = results.get_count()
                print(f"📈 Total Indexed Chunks: {total_indexed}")
                
                if total_indexed > 0:
                    # Get sample documents to analyze structure
                    print("Analyzing document structure...")
                    sample_results = search_client.search(
                        search_text="*",
                        select="pdf_id,chunk_index,chunk_total",
                        top=100  # Reduced from 1000 to prevent timeout
                    )
                    
                    # Count unique documents
                    unique_documents = set()
                    chunk_stats = defaultdict(int)
                    
                    for result in sample_results:
                        doc_id = result.get("pdf_id")
                        if doc_id:
                            unique_documents.add(doc_id)
                        
                        chunk_total = result.get("chunk_total", 0)
                        if chunk_total:
                            chunk_stats[chunk_total] += 1
                    
                    print(f"📄 Unique Documents Indexed: {len(unique_documents)}")
                    
                    if chunk_stats:
                        print(f"\n📊 Chunk Distribution:")
                        for chunk_count, doc_count in sorted(chunk_stats.items()):
                            print(f"   • {chunk_count} chunks: {doc_count} documents")
                
                # Check index health
                print(f"\n✅ Search index '{self.config.AZURE_SEARCH_INDEX_NAME}' is accessible")
                
                return {
                    "total_chunks": total_indexed,
                    "unique_documents": len(unique_documents) if total_indexed > 0 else 0,
                    "status": "healthy"
                }
                
            except Exception as e:
                print(f"❌ Error querying search index: {e}")
                return {"error": str(e)}
                
        except Exception as e:
            print(f"❌ Error checking search index status: {e}")
            return {"error": str(e)}
    
    def check_pipeline_consistency(self, cosmos_stats, search_stats):
        """Check consistency between Cosmos DB and Search Index"""
        print("\n" + "="*60)
        print("🔄 PIPELINE CONSISTENCY CHECK")
        print("="*60)
        
        if "error" in cosmos_stats or "error" in search_stats:
            print("❌ Cannot perform consistency check due to errors in data retrieval")
            return
        
        cosmos_with_metadata = cosmos_stats.get("with_metadata", 0)
        indexed_documents = search_stats.get("unique_documents", 0)
        
        print(f"📊 Documents with metadata (Cosmos): {cosmos_with_metadata}")
        print(f"📊 Documents indexed (Search): {indexed_documents}")
        
        if cosmos_with_metadata == indexed_documents:
            print("✅ Perfect consistency - all documents with metadata are indexed")
        elif cosmos_with_metadata > indexed_documents:
            missing = cosmos_with_metadata - indexed_documents
            print(f"⚠️  {missing} documents have metadata but are not indexed")
            print("   💡 Consider running indexing phase to sync up")
        elif indexed_documents > cosmos_with_metadata:
            extra = indexed_documents - cosmos_with_metadata
            print(f"⚠️  {extra} documents are indexed but missing metadata")
            print("   💡 This might indicate orphaned index entries")
        
        # Calculate overall progress
        if cosmos_stats.get("total", 0) > 0:
            overall_progress = (cosmos_with_metadata / cosmos_stats["total"]) * 100
            print(f"\n📈 Overall Pipeline Progress: {overall_progress:.1f}%")
            
            if overall_progress < 50:
                print("   🔴 Low progress - consider running metadata extraction")
            elif overall_progress < 90:
                print("   🟡 Good progress - pipeline is running")
            else:
                print("   🟢 Excellent progress - pipeline nearly complete")
    
    def generate_detailed_report(self):
        """Generate a detailed status report"""
        print("\n" + "="*80)
        print("📋 DETAILED PIPELINE STATUS REPORT")
        print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        # Check all components
        cosmos_stats = self.check_cosmos_status()
        search_stats = self.check_search_index_status()
        
        # Consistency check
        self.check_pipeline_consistency(cosmos_stats, search_stats)
        
        # Summary recommendations
        print("\n" + "="*60)
        print("💡 RECOMMENDATIONS")
        print("="*60)
        
        if cosmos_stats.get("without_metadata", 0) > 0:
            print("• Run metadata extraction for documents without metadata")
        
        if cosmos_stats.get("with_metadata", 0) > search_stats.get("unique_documents", 0):
            print("• Run indexing phase to sync documents to search index")
        
        if search_stats.get("total_chunks", 0) == 0:
            print("• Search index is empty - run complete pipeline")
        
        print("• Use cleaning scripts if you need to reset any component")
        
        return {
            "cosmos": cosmos_stats,
            "search": search_stats,
            "timestamp": datetime.now().isoformat()
        }
    
    def export_report(self, output_file="pipeline_status_report.json"):
        """Export status report to JSON file"""
        try:
            report = self.generate_detailed_report()
            
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            print(f"\n💾 Report exported to: {output_file}")
            return True
            
        except Exception as e:
            print(f"❌ Error exporting report: {e}")
            return False
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            self.azure_clients.cleanup()
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Check PDF Processing Pipeline Status")
    parser.add_argument("--export", help="Export report to JSON file", metavar="FILENAME")
    parser.add_argument("--cosmos-only", action="store_true", help="Check only Cosmos DB status")
    parser.add_argument("--search-only", action="store_true", help="Check only Search Index status")
    
    args = parser.parse_args()
    
    checker = None
    try:
        checker = PipelineStatusChecker()
        
        if args.cosmos_only:
            checker.check_cosmos_status()
        elif args.search_only:
            checker.check_search_index_status()
        else:
            if args.export:
                checker.export_report(args.export)
            else:
                checker.generate_detailed_report()
        
        return 0
        
    except KeyboardInterrupt:
        print("\n❌ Status check interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Error during status check: {e}")
        return 1
    finally:
        if checker:
            checker.cleanup()


if __name__ == "__main__":
    sys.exit(main())
