"""Business logic services for Icecat integration."""

from .assortment_reader import AssortmentReader, AssortmentItem, AssortmentStats
from .product_matcher import ProductMatcher, MatchResult, BatchMatchResult
from .product_sync_service import ProductSyncService, SyncResult
from .batch_processor import BatchProcessor, BatchResult, ProcessingStats
from .sync_orchestrator import SyncOrchestrator, SyncRunResult
from .taxonomy_update_service import TaxonomyUpdateService, TaxonomyUpdateStats
from .daily_index_service import DailyIndexService, DailyIndexResult
from .comparison_service import ComparisonService, ComparisonResult, FieldDifference

__all__ = [
    # Assortment reader
    "AssortmentReader",
    "AssortmentItem",
    "AssortmentStats",
    # Product matcher
    "ProductMatcher",
    "MatchResult",
    "BatchMatchResult",
    # Sync service
    "ProductSyncService",
    "SyncResult",
    # Batch processor
    "BatchProcessor",
    "BatchResult",
    "ProcessingStats",
    # Orchestrator
    "SyncOrchestrator",
    "SyncRunResult",
    # Taxonomy
    "TaxonomyUpdateService",
    "TaxonomyUpdateStats",
    # Daily Index
    "DailyIndexService",
    "DailyIndexResult",
    # Comparison
    "ComparisonService",
    "ComparisonResult",
    "FieldDifference",
]
